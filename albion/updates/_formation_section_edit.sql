-- =========================================================
-- formation_section_edit v17.4 (parámetros embebidos)
--  * Toma (section_id, hole_id) desde albion.hole_section (igual v17).
--  * Cálculo automático de from_/to_:
--      - Proyección del sketch sobre la línea 2D de albion.hole_section (sid,hid).
--      - Reglas automáticas:
--          · if from_ < 1 => from_ = 0
--          · else from_ a 1 decimal
--          · to_ a 1 decimal
--          · eps mínimo = 0.01
--      - Anti-huecos (solo automáticos, nunca manuales):
--          · Amarra from_ al to_ inmediatamente superior (to_ <= from_) si |diff| <= depth_snap
--          · Amarra to_ al from_ inmediatamente inferior (from_ >= to_) si |diff| <= depth_snap
--          · Ignora “fantasmas” por patrón fijo en code/comments (ghost_pat)
--  * Parámetros embebidos (edítalos aquí y vuelve a aplicar el SQL):
--      - depth_snap = 0.10
--      - ghost_pat  = 'topo%'
-- =========================================================

DROP TRIGGER IF EXISTS formation_section_edit_instead_trig ON albion.formation_section_edit;
DROP VIEW IF EXISTS albion.formation_section_edit CASCADE;
DROP FUNCTION IF EXISTS albion.formation_section_edit_instead_fct() CASCADE;

-- Vista basada en hole_section (igual a v17)

CREATE VIEW albion.formation_section_edit AS
SELECT
  (f.id || ' ' || hs.section_id)::varchar          AS id,          -- PK estable/único para QGIS
  f.id::varchar                                     AS formation_id,
  f.code                                            AS code,
  f.comments                                        AS comments,
  hs.hole_id                                        AS hole_id,
  hs.section_id                                     AS section_id,
  f.from_                                           AS from_,
  f.to_                                             AS to_,
  ST_Force3D(
    albion.to_section(
      albion.hole_piece(f.from_, f.to_, hs.hole_id),
      s.anchor, s.scale
    )
  )::geometry('LINESTRINGZ', $SRID)                 AS geom
FROM albion.hole_section hs
JOIN _albion.section  s ON s.id = hs.section_id
JOIN _albion.formation f ON f.hole_id = hs.hole_id;

-- 1) Eliminar trigger y vista para poder cambiar el tipo de "id"
DROP TRIGGER IF EXISTS formation_section_instead_trig ON albion.formation_section;
DROP VIEW IF EXISTS albion.formation_section CASCADE;

-- 2) Recrear la vista con id estable y único por fila
--    id = formation_id || ' ' || section_id
CREATE VIEW albion.formation_section AS
SELECT
  (t.id || ' ' || sc.section_id)::varchar AS id,                -- ID estable/único (usará QGIS)
  t.id::varchar                           AS formation_id,
  sc.section_id::varchar                  AS section_id,
  t.hole_id::varchar                      AS hole_id,
  t.from_::real                           AS from_,
  t.to_::real                             AS to_,
  sc.geom::geometry('LINESTRING', $SRID)  AS geom,
  t.code,
  t.comments,
  (t.id || ' ' || sc.section_id)::varchar AS uid                -- dejas uid también, por compatibilidad
FROM _albion.formation t
JOIN albion.formation_section_geom_cache sc
  ON sc.formation_id = t.id
JOIN _albion.section s
  ON ST_Intersects(s.geom, sc.collar) AND sc.section_id = s.id;
-- Nota: la vista replicará una formation en cada sección donde exista hole_section para ese hole_id.
-- En QGIS normalmente filtras por section_id cuando editas.

-- Trigger INSTEAD OF con anti-huecos y parámetros embebidos
CREATE OR REPLACE FUNCTION albion.formation_section_edit_instead_fct()
RETURNS trigger
LANGUAGE plpgsql
AS
$$
DECLARE
    -- Candidato tomado de albion.hole_section
    hs_section_id   varchar;
    hs_hole_id      varchar;
    hs_found        boolean := false;

    -- Resultado final
    sid varchar;
    hid varchar;

    -- Geometrías / escala de la sección
    a   geometry;
    sg  geometry;
    sc  real;

    -- Pozo (para depth_)
    hs_geom geometry;
    dpt     real;

    -- Línea de hole_section para (sid,hid) en 2D (referencia)
    hs_sec2d geometry;

    -- Cálculo profundidades
    draw2d geometry;
    t0     double precision;
    t1     double precision;
    f0     real;
    f1     real;
    f0_auto real;
    f1_auto real;
    eps    real := 0.01;

    -- Flags: entrada manual
    manual_from boolean := false;
    manual_to   boolean := false;

    -- Parámetros embebidos (edítalos aquí si quieres cambiar comportamiento)
    depth_snap real := 0.10;     -- tolerancia vertical de amarre (mismas unidades que depth_)
    ghost_pat  text := 'topo%';  -- patrón para ignorar “fantasmas” en code/comments (ILIKE)

    -- Vecinos para amarre
    near_to    real;
    near_from  real;

    -- Texto seguro
    code_txt         text;
    comments_txt     text;
    old_code_txt     text;
    old_comments_txt text;

    -- Compat (no usado por anti-huecos; se mantiene por retro)
    fs_snap_base real := 0.5;
    fs_snap      real;
    existing_comment text;
BEGIN
    IF TG_OP IN ('INSERT','UPDATE') THEN
        NEW.geom := ST_Force3D(NEW.geom);
        draw2d   := ST_Force2D(NEW.geom);

        manual_from := (NEW.from_ IS NOT NULL);
        manual_to   := (NEW.to_   IS NOT NULL);

        code_txt     := NEW.code::text;
        comments_txt := CASE WHEN NEW.comments IS NULL THEN NULL ELSE NEW.comments::text END;
        IF TG_OP = 'UPDATE' THEN
            old_code_txt     := OLD.code::text;
            old_comments_txt := CASE WHEN OLD.comments IS NULL THEN NULL ELSE OLD.comments::text END;
        END IF;

        -- Compat: snap base (no impacta el anti-huecos)
        fs_snap := fs_snap_base;

        ------------------------------------------------------------
        -- 1) Buscar candidato en albion.hole_section
        --    - Si NEW.hole_id viene informado, prioriza ese hole.
        --    - Orden: intersecta (TRUE primero) luego menor distancia.
        ------------------------------------------------------------
        IF NEW.hole_id IS NOT NULL THEN
            SELECT hs.section_id, hs.hole_id
              INTO hs_section_id, hs_hole_id
            FROM albion.hole_section hs
            WHERE hs.hole_id = NEW.hole_id
            ORDER BY
              (ST_Intersects(ST_Force2D(hs.geom), draw2d)) DESC,
              ST_Distance(ST_Force2D(hs.geom), draw2d) ASC
            LIMIT 1;
        ELSE
            SELECT hs.section_id, hs.hole_id
              INTO hs_section_id, hs_hole_id
            FROM albion.hole_section hs
            ORDER BY
              (ST_Intersects(ST_Force2D(hs.geom), draw2d)) DESC,
              ST_Distance(ST_Force2D(hs.geom), draw2d) ASC
            LIMIT 1;
        END IF;

        hs_found := hs_section_id IS NOT NULL;

        ------------------------------------------------------------
        -- 2) Asignar sección / pozo desde hole_section si existe
        ------------------------------------------------------------
        sid := COALESCE(hs_section_id, NEW.section_id);
        hid := COALESCE(hs_hole_id,   NEW.hole_id);

        ------------------------------------------------------------
        -- 3) Fallback si NO hubo candidato en hole_section
        ------------------------------------------------------------
        IF NOT hs_found THEN
            IF sid IS NULL THEN
                SELECT s.id INTO sid
                FROM _albion.section s
                ORDER BY ST_Distance(s.geom, ST_StartPoint(draw2d)) ASC
                LIMIT 1;
            END IF;

            IF sid IS NULL THEN
                RAISE EXCEPTION 'No se pudo determinar section_id.';
            END IF;

            IF hid IS NULL THEN
                SELECT hs.hole_id INTO hid
                FROM albion.hole_section hs
                WHERE hs.section_id = sid
                ORDER BY ST_Distance(ST_Force2D(hs.geom), draw2d) ASC
                LIMIT 1;
            END IF;
            IF hid IS NULL THEN
                RAISE EXCEPTION 'No se pudo determinar hole_id.';
            END IF;
        END IF;

        ------------------------------------------------------------
        -- 4) Cargar sección, pozo (depth) y línea hole_section (2D)
        ------------------------------------------------------------
        SELECT anchor, geom, scale INTO a, sg, sc
        FROM _albion.section
        WHERE id = sid;

        SELECT geom, depth_ INTO hs_geom, dpt
        FROM _albion.hole
        WHERE id = hid;

        IF dpt IS NULL THEN
            RAISE EXCEPTION 'El pozo % no tiene depth_ definido', hid;
        END IF;

        SELECT ST_Force2D(hs.geom)
          INTO hs_sec2d
        FROM albion.hole_section hs
        WHERE hs.section_id = sid
          AND hs.hole_id    = hid
        ORDER BY ST_Distance(ST_Force2D(hs.geom), draw2d) ASC
        LIMIT 1;

        IF hs_sec2d IS NULL THEN
            RAISE EXCEPTION 'No existe geom en albion.hole_section para section_id % y hole_id %', sid, hid;
        END IF;

        ------------------------------------------------------------
        -- 5) Calcular from_/to_ sólo si faltan (modo automático)
        --    Proyección sobre hs_sec2d + aproximaciones + anti-huecos.
        ------------------------------------------------------------
        IF NEW.from_ IS NULL OR NEW.to_ IS NULL THEN
            -- Fracciones [0..1] sobre la línea de referencia
            t0 := ST_LineLocatePoint(hs_sec2d, ST_StartPoint(draw2d));
            t1 := ST_LineLocatePoint(hs_sec2d, ST_EndPoint(draw2d));

            f0_auto := LEAST(GREATEST(LEAST(t0,t1) * dpt, 0), dpt);
            f1_auto := LEAST(GREATEST(GREATEST(t0,t1) * dpt, 0), dpt);

            -- Lado FROM (automático si no fue escrito por el usuario)
            IF NOT manual_from THEN
                IF f0_auto < 1 THEN
                    f0 := 0;
                ELSE
                    f0 := ROUND(f0_auto::numeric, 1)::real; -- 1 decimal
                    f0 := LEAST(GREATEST(f0, 0), dpt);
                END IF;
            ELSE
                f0 := NEW.from_;
            END IF;

            -- Lado TO (automático si no fue escrito por el usuario)
            IF NOT manual_to THEN
                f1 := ROUND(f1_auto::numeric, 1)::real;     -- 1 decimal
                f1 := LEAST(GREATEST(f1, 0), dpt);
            ELSE
                f1 := NEW.to_;
            END IF;

            -- Espesor mínimo preliminar
            IF f1 - f0 < eps THEN
                IF NOT manual_to THEN
                    f1 := LEAST(ROUND((f0 + eps)::numeric,1)::real, dpt);
                    IF f1 - f0 < eps THEN
                        f0 := GREATEST(f1 - eps, 0);
                    END IF;
                ELSIF NOT manual_from THEN
                    f0 := GREATEST(ROUND((f1 - eps)::numeric,1)::real, 0);
                    IF f1 - f0 < eps THEN
                        f1 := LEAST(f0 + eps, dpt);
                    END IF;
                ELSE
                    f1 := LEAST(f0 + eps, dpt);
                    IF f1 - f0 < eps THEN
                        f0 := GREATEST(f1 - eps, 0);
                    END IF;
                END IF;
            END IF;

            --------------------------------------------------------
            -- 5.1) Anti-huecos: amarre con dirección y tolerancia
            --      (Solo si ese lado fue automático; IGNORA “fantasmas”)
            --------------------------------------------------------
            IF NOT manual_from THEN
                -- Buscar el to_ inmediatamente superior (to_ <= f0), ignorando fantasmas
                SELECT f2.to_
                  INTO near_to
                FROM _albion.formation f2
                WHERE f2.hole_id = hid
                  AND (TG_OP <> 'UPDATE' OR f2.id <> OLD.formation_id)
                  AND NOT (COALESCE(f2.code::text,'') ILIKE ghost_pat
                           OR COALESCE(f2.comments,'') ILIKE ghost_pat)
                  AND f2.to_ <= f0
                ORDER BY (f0 - f2.to_) ASC
                LIMIT 1;

                IF near_to IS NOT NULL AND (f0 - near_to) <= depth_snap THEN
                    f0 := near_to;   -- amarre EXACTO al vecino de arriba
                END IF;
            END IF;

            IF NOT manual_to THEN
                -- Buscar el from_ inmediatamente inferior (from_ >= f1), ignorando fantasmas
                SELECT f2.from_
                  INTO near_from
                FROM _albion.formation f2
                WHERE f2.hole_id = hid
                  AND (TG_OP <> 'UPDATE' OR f2.id <> OLD.formation_id)
                  AND NOT (COALESCE(f2.code::text,'') ILIKE ghost_pat
                           OR COALESCE(f2.comments,'') ILIKE ghost_pat)
                  AND f2.from_ >= f1
                ORDER BY (f2.from_ - f1) ASC
                LIMIT 1;

                IF near_from IS NOT NULL AND (near_from - f1) <= depth_snap THEN
                    f1 := near_from; -- amarre EXACTO al vecino de abajo
                END IF;
            END IF;

            -- Validación final de espesor
            IF f1 - f0 < eps THEN
                RAISE EXCEPTION 'El espacio disponible (%.3f..%.3f) es menor que eps %.3f; ajuste el dibujo o la tolerancia depth_snap=%',
                    f0, f1, eps, depth_snap;
            END IF;

        ELSE
            -- Ambos provienen del usuario: respetar (sin redondeos/amarres)
            f0 := NEW.from_;
            f1 := NEW.to_;
            IF f1 - f0 < eps THEN
                f1 := LEAST(f0 + eps, dpt);
                IF f1 - f0 < eps THEN
                    f0 := GREATEST(f1 - eps, 0);
                END IF;
            END IF;
        END IF;

        ------------------------------------------------------------
        -- 6) Lookup de comments (solo si vacío / no personalizado)
        ------------------------------------------------------------
        IF TG_OP = 'INSERT' THEN
            IF (comments_txt IS NULL OR btrim(comments_txt) = '')
               AND (code_txt IS NOT NULL AND btrim(code_txt) <> '') THEN
                SELECT f.comments INTO existing_comment
                FROM _albion.formation f
                WHERE f.code = NEW.code
                  AND f.comments IS NOT NULL
                  AND btrim(f.comments::text) <> ''
                ORDER BY f.from_ ASC
                LIMIT 1;

                IF existing_comment IS NOT NULL THEN
                    NEW.comments := existing_comment;
                ELSE
                    NEW.comments := NULL; -- sin fallback a code
                END IF;
            END IF;
        ELSE
            -- En UPDATE si cambia el code y comments sigue sin personalizar
            IF (code_txt IS DISTINCT FROM old_code_txt)
               AND (
                    comments_txt IS NULL
                 OR btrim(comments_txt) = ''
                 OR comments_txt = old_comments_txt
               )
               AND (
                    old_comments_txt IS NULL
                 OR btrim(old_comments_txt) = ''
                 OR old_comments_txt = old_code_txt
               ) THEN
                SELECT f.comments INTO existing_comment
                FROM _albion.formation f
                WHERE f.code = NEW.code
                  AND f.comments IS NOT NULL
                  AND btrim(f.comments::text) <> ''
                ORDER BY f.from_ ASC
                LIMIT 1;

                IF existing_comment IS NOT NULL THEN
                    NEW.comments := existing_comment;
                ELSE
                    NEW.comments := NULL;
                END IF;
            END IF;
        END IF;

        ------------------------------------------------------------
        -- 7) Normalizar NEW
        ------------------------------------------------------------
        NEW.section_id := sid;
        NEW.hole_id    := hid;
        NEW.from_      := f0;
        NEW.to_        := f1;
    END IF;

    ------------------------------------------------------------
    -- 8) DML subyacente (igual v17)
    ------------------------------------------------------------
    IF TG_OP = 'INSERT' THEN
        INSERT INTO _albion.formation(id, hole_id, code, from_, to_, comments)
        VALUES (_albion.unique_id()::varchar, NEW.hole_id, NEW.code, NEW.from_, NEW.to_, NEW.comments)
        RETURNING id INTO NEW.formation_id;
        RETURN NEW;

    ELSIF TG_OP = 'UPDATE' THEN
        UPDATE _albion.formation
           SET code     = NEW.code,
               hole_id  = NEW.hole_id,
               from_    = NEW.from_,
               to_      = NEW.to_,
               comments = NEW.comments
         WHERE id = OLD.formation_id;
        RETURN NEW;

    ELSIF TG_OP = 'DELETE' THEN
        DELETE FROM _albion.formation WHERE id = OLD.formation_id;
        RETURN OLD;
    END IF;

    RETURN NULL;
END;
$$;

CREATE TRIGGER formation_section_edit_instead_trig
INSTEAD OF INSERT OR UPDATE OR DELETE ON albion.formation_section_edit
FOR EACH ROW
EXECUTE PROCEDURE albion.formation_section_edit_instead_fct();
