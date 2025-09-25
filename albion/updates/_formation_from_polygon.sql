-- =========================================================
-- Bulk from polygon → temp_lines (para pegar en formation_section_edit)
-- - Crea albion.polygon_edit (Polygon) con campo code.
-- - Crea albion.temp_lines (LineString) con solo code y geom no nulos.
-- - Trigger: al insertar/editar/borrar en polygon_edit, se regeneran
--   las líneas correspondientes en temp_lines intersectando con albion.hole_section.
--
-- Requisitos:
--   - PostGIS instalado
--   - Existe albion.hole_section (geom LineString, SRID 9377)
--   - Existe _albion.unique_id() que devuelve un identificador único (varchar/uuid)
--
-- Notas:
--   - No se llena section_id, hole_id, from_, to_, comments: quedan NULL.
--   - SRID=9377. Ajustar si tu proyecto usa otro.
-- =========================================================

-- Limpieza previa (idempotencia)
DROP TRIGGER IF EXISTS polygon_edit_aiud_trig ON albion.polygon_edit;
DROP FUNCTION IF EXISTS albion.polygon_edit_aiud_trg() CASCADE;
DROP FUNCTION IF EXISTS albion._polygon_edit_sync_temp_lines(p_polygon_id varchar) CASCADE;

-- Capa de entrada: corredores con code
CREATE TABLE IF NOT EXISTS albion.polygon_edit (
    id         varchar PRIMARY KEY DEFAULT _albion.unique_id()::varchar,
    code       integer NOT NULL,
    comments   text,
    geom       geometry(Polygon, 9377) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Asegurar SRID correcto
ALTER TABLE albion.polygon_edit
  ADD CONSTRAINT polygon_edit_geom_srid CHECK (ST_SRID(geom) = 9377);

CREATE INDEX IF NOT EXISTS sidx_polygon_edit_geom ON albion.polygon_edit USING gist (geom);

-- Autoupdate de updated_at
CREATE OR REPLACE FUNCTION albion._touch_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS polygon_edit_touch_trg ON albion.polygon_edit;
CREATE TRIGGER polygon_edit_touch_trg
BEFORE UPDATE ON albion.polygon_edit
FOR EACH ROW
EXECUTE PROCEDURE albion._touch_updated_at();

-- Capa de salida: líneas temporales para copiar/pegar
-- IMPORTANTE: Evitamos campos "id" o "formation_id" para no interferir con el paste.
CREATE TABLE IF NOT EXISTS albion.temp_lines (
    uid        varchar PRIMARY KEY DEFAULT _albion.unique_id()::varchar,
    polygon_id varchar NOT NULL,
    code       integer NOT NULL,
    comments   text,        -- opcional, queda NULL
    section_id varchar,     -- queda NULL
    hole_id    varchar,     -- queda NULL
    from_      real,        -- queda NULL
    to_        real,        -- queda NULL
    geom       geometry(LineString, 9377) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE albion.temp_lines
  ADD CONSTRAINT temp_lines_geom_srid CHECK (ST_SRID(geom) = 9377);

CREATE INDEX IF NOT EXISTS idx_temp_lines_polygon_id ON albion.temp_lines (polygon_id);
CREATE INDEX IF NOT EXISTS sidx_temp_lines_geom ON albion.temp_lines USING gist (geom);

-- Función: reconstruye las líneas de un polígono dado
CREATE OR REPLACE FUNCTION albion._polygon_edit_sync_temp_lines(p_polygon_id varchar)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  v_code    integer;
  v_cmt     text;
  v_poly    geometry(Polygon, 9377);
  v_inserted integer := 0;

  -- Parámetros editables (si hiciera falta):
  v_buffer_m   double precision := 0.0;  -- buffer opcional del polígono (m). 0.0 = intersección exacta
  v_min_length double precision := 0.0;  -- longitud mínima de segmento en sección (m). 0.0 = sin filtro
BEGIN
  -- Leer polígono
  SELECT code, comments, geom
    INTO v_code, v_cmt, v_poly
  FROM albion.polygon_edit
  WHERE id = p_polygon_id;

  -- Si no existe (p.ej. DELETE), limpiar y salir
  IF NOT FOUND THEN
    DELETE FROM albion.temp_lines WHERE polygon_id = p_polygon_id;
    RETURN 0;
  END IF;

  -- Borrar líneas previas del mismo polígono
  DELETE FROM albion.temp_lines WHERE polygon_id = p_polygon_id;

  -- Insertar nuevas líneas a partir de intersección con hole_section
  INSERT INTO albion.temp_lines (uid, polygon_id, code, comments, section_id, hole_id, from_, to_, geom)
  SELECT
      _albion.unique_id()::varchar        AS uid,
      p_polygon_id                         AS polygon_id,
      v_code                               AS code,
      NULL::text                           AS comments,    -- forzar NULL
      NULL::varchar                        AS section_id,  -- forzar NULL
      NULL::varchar                        AS hole_id,     -- forzar NULL
      NULL::real                           AS from_,       -- forzar NULL
      NULL::real                           AS to_,         -- forzar NULL
      ST_SetSRID(ST_LineMerge(d.geom), 9377) AS geom
  FROM (
      SELECT (ST_Dump(
                ST_CollectionExtract(
                  ST_Intersection(
                    ST_Buffer(ST_Force2D(v_poly), v_buffer_m),
                    ST_Force2D(hs.geom)
                  ),
                  2  -- 2 = LineString
                )
              )).geom AS geom
      FROM albion.hole_section hs
      WHERE ST_Intersects(
              ST_Force2D(hs.geom),
              ST_Buffer(ST_Force2D(v_poly), v_buffer_m)
            )
  ) AS d
  WHERE
      ST_NPoints(ST_LineMerge(d.geom)) >= 2
      AND ST_Length(ST_LineMerge(d.geom)) >= v_min_length;

  GET DIAGNOSTICS v_inserted = ROW_COUNT;
  RETURN v_inserted;
END;
$$;

-- Trigger por-row: al insertar/editar/borrar un polígono, sincroniza sus líneas
CREATE OR REPLACE FUNCTION albion.polygon_edit_aiud_trg()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    PERFORM albion._polygon_edit_sync_temp_lines(NEW.id);
    RETURN NEW;

  ELSIF TG_OP = 'UPDATE' THEN
    -- Si cambian geom o code, regenerar
    IF (NEW.geom IS DISTINCT FROM OLD.geom) OR (NEW.code IS DISTINCT FROM OLD.code) THEN
      PERFORM albion._polygon_edit_sync_temp_lines(NEW.id);
    END IF;
    RETURN NEW;

  ELSIF TG_OP = 'DELETE' THEN
    DELETE FROM albion.temp_lines WHERE polygon_id = OLD.id;
    RETURN OLD;
  END IF;

  RETURN NULL;
END;
$$;

CREATE TRIGGER polygon_edit_aiud_trig
AFTER INSERT OR UPDATE OR DELETE ON albion.polygon_edit
FOR EACH ROW
EXECUTE PROCEDURE albion.polygon_edit_aiud_trg();

-- (Opcional) Utilidad para reconstruir todo
CREATE OR REPLACE FUNCTION albion.rebuild_all_temp_lines()
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  r record;
  total integer := 0;
BEGIN
  DELETE FROM albion.temp_lines;
  FOR r IN SELECT id FROM albion.polygon_edit LOOP
    total := total + COALESCE(albion._polygon_edit_sync_temp_lines(r.id), 0);
  END LOOP;
  RETURN total;
END;
$$;