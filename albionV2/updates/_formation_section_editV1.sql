-- Vista editable para dibujar intervalos de formación directamente en sección.
-- Lo dibujado se guarda en _albion.formation, aplicando un espesor mínimo (eps) de 0.01.
-- Luego albion.formation_section reflejará automáticamente lo nuevo.

-- 1) Vista de edición en sección
CREATE OR REPLACE VIEW albion.formation_section_edit AS
SELECT
    row_number() OVER ()             AS id,            -- índice visual
    f.id                              AS formation_id, -- PK estable de la formación
    f.code                            AS code,
    h.id                              AS hole_id,
    s.id                              AS section_id,
    f.from_                           AS from_,
    f.to_                             AS to_,
    (
      albion.to_section(
        albion.hole_piece(f.from_, f.to_, h.id),
        s.anchor, s.scale
      )
    )::geometry('LINESTRING', $SRID)  AS geom
FROM _albion.section s
JOIN _albion.hole    h ON s.geom && h.geom AND ST_Intersects(ST_StartPoint(h.geom), s.geom)
JOIN _albion.formation f ON f.hole_id = h.id
;

-- 2) Trigger INSTEAD OF para insertar/editar/borrar “dibujando” en sección
CREATE OR REPLACE FUNCTION albion.formation_section_edit_instead_fct()
RETURNS trigger
LANGUAGE plpgsql
AS
$$
DECLARE
    -- Sección candidata
    a    geometry;  -- anchor
    sg   geometry;  -- línea de sección
    sc   real;      -- scale Z
    sid  varchar;   -- section_id efectivo

    -- Pozo candidato
    hs   geometry;  -- geom del pozo (3D)
    dpt  real;      -- profundidad total del pozo
    hid  varchar;   -- hole_id efectivo

    -- Geometrías y medidas
    g3d  geometry;  -- desproyección de NEW.geom a 3D
    t0   double precision;
    t1   double precision;
    f0   real;      -- from_
    f1   real;      -- to_
    eps  real := 0.01;  -- espesor mínimo “fantasma”

    snap real;      -- distancia de snap desde metadata
BEGIN
    IF TG_OP IN ('INSERT','UPDATE') THEN
        -- 2.1 Derivar section_id si viene NULL: usamos la sección más cercana al trazo
        sid := COALESCE(
            NEW.section_id,
            (
              SELECT s.id
              FROM _albion.section s
              ORDER BY ST_Distance(s.geom, ST_StartPoint(NEW.geom)) ASC
              LIMIT 1
            )
        );

        -- Cargar geometría y escala de la sección
        SELECT anchor, geom, scale INTO a, sg, sc
        FROM _albion.section
        WHERE id = sid;

        -- 2.2 Derivar hole_id si viene NULL: buscamos el pozo más cercano en la misma sección
        SELECT snap_distance INTO snap FROM _albion.metadata;

        hid := COALESCE(
            NEW.hole_id,
            (
              SELECT hs.hole_id
              FROM albion.hole_section hs
              WHERE hs.section_id = sid
              ORDER BY ST_Distance(hs.geom, NEW.geom) ASC
              LIMIT 1
            )
        );

        -- Validación: debe existir pozo en contexto de sección
        IF hid IS NULL THEN
            RAISE EXCEPTION 'No se pudo determinar hole_id para la geometría dibujada.';
        END IF;

        -- Cargar geom y profundidad del pozo
        SELECT geom, depth_ INTO hs, dpt
        FROM _albion.hole
        WHERE id = hid;

        -- 2.3 Si no vienen from_/to_, los derivamos por desproyección + localización sobre el pozo
        IF NEW.from_ IS NULL OR NEW.to_ IS NULL THEN
            g3d := albion.from_section(NEW.geom, a, sg, sc);

            t0 := ST_LineLocatePoint(hs, ST_StartPoint(g3d));
            t1 := ST_LineLocatePoint(hs, ST_EndPoint(g3d));

            f0 := LEAST(GREATEST(LEAST(t0,t1) * dpt, 0), dpt);
            f1 := LEAST(GREATEST(GREATEST(t0,t1) * dpt, 0), dpt);
        ELSE
            f0 := NEW.from_;
            f1 := NEW.to_;
        END IF;

        -- 2.4 Forzar espesor mínimo
        IF f1 - f0 < eps THEN
            f1 := LEAST(f0 + eps, dpt);
            IF f1 - f0 < eps THEN
                f0 := GREATEST(f1 - eps, 0);
            END IF;
        END IF;

        -- Normalizar salida NEW por si el cliente espera eco
        NEW.section_id := sid;
        NEW.hole_id    := hid;
        NEW.from_      := f0;
        NEW.to_        := f1;
    END IF;

    IF TG_OP = 'INSERT' THEN
        INSERT INTO _albion.formation(id, hole_id, code, from_, to_, comments)
        VALUES (_albion.unique_id()::varchar, NEW.hole_id, NEW.code, NEW.from_, NEW.to_, NULL)
        RETURNING id INTO NEW.formation_id;
        RETURN NEW;

    ELSIF TG_OP = 'UPDATE' THEN
        UPDATE _albion.formation
        SET code = NEW.code,
            hole_id = NEW.hole_id,
            from_ = NEW.from_,
            to_ = NEW.to_
        WHERE id = OLD.formation_id;
        RETURN NEW;

    ELSIF TG_OP = 'DELETE' THEN
        DELETE FROM _albion.formation
        WHERE id = OLD.formation_id;
        RETURN OLD;
    END IF;
END;
$$;

DROP TRIGGER IF EXISTS formation_section_edit_instead_trig ON albion.formation_section_edit;
CREATE TRIGGER formation_section_edit_instead_trig
INSTEAD OF INSERT OR UPDATE OR DELETE ON albion.formation_section_edit
FOR EACH ROW EXECUTE PROCEDURE albion.formation_section_edit_instead_fct();