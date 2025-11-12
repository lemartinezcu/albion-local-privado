-- =========================================================
-- formation_section_edit v16
--  * Selección de section_id y hole_id desde albion.formation_section
--    (prioriza intersección, luego menor distancia).
--  * Fallback solo si no se encuentra candidato.
--  * Lookup de comments por code (si viene vacío). Sin fallback a copiar code.
--  * Mantiene lógica original de from_/to_ (solo si NULL) + eps=0.01.
--  * Geometría estándar LINESTRINGZ (forzado a 3D en vista y trigger).
-- =========================================================

DROP TRIGGER IF EXISTS formation_section_edit_instead_trig ON albion.formation_section_edit;
DROP VIEW IF EXISTS albion.formation_section_edit CASCADE;
DROP FUNCTION IF EXISTS albion.formation_section_edit_instead_fct() CASCADE;

-- Vista: igual a versiones previas con comments incluido, proyectando a sección y forzando 3D
CREATE OR REPLACE VIEW albion.formation_section_edit AS
SELECT
    row_number() OVER ()                              AS id,
    f.id                                              AS formation_id,
    f.code                                            AS code,
    f.comments                                        AS comments,
    h.id                                              AS hole_id,
    s.id                                              AS section_id,
    f.from_                                           AS from_,
    f.to_                                             AS to_,
    ST_Force3D(
      albion.to_section(
        albion.hole_piece(f.from_, f.to_, h.id),
        s.anchor, s.scale
      )
    )::geometry('LINESTRINGZ', $SRID)                 AS geom
FROM _albion.section s
JOIN _albion.hole       h ON s.geom && h.geom AND ST_Intersects(ST_StartPoint(h.geom), s.geom)
JOIN _albion.formation  f ON f.hole_id = h.id;

-- Trigger INSTEAD OF
CREATE OR REPLACE FUNCTION albion.formation_section_edit_instead_fct()
RETURNS trigger
LANGUAGE plpgsql
AS
$$
DECLARE
    -- Candidato tomado de albion.formation_section
    fs_section_id   varchar;
    fs_hole_id      varchar;
    fs_found        boolean := false;

    -- Resultado final
    sid varchar;
    hid varchar;

    -- Geometrías / escala de la sección
    a   geometry;
    sg  geometry;
    sc  real;

    -- Pozo
    hs  geometry;
    dpt real;

    -- Cálculo profundidades
    draw2d geometry;
    g3d    geometry;
    t0     double precision;
    t1     double precision;
    f0     real;
    f1     real;
    eps    real := 0.01;

    -- Texto seguro
    code_txt         text;
    comments_txt     text;
    old_code_txt     text;
    old_comments_txt text;

    -- Parámetros
    fs_snap_base real := 0.5;  -- intentamos leer metadata; default 0.5
    fs_snap      real;
    existing_comment text;
BEGIN
    IF TG_OP IN ('INSERT','UPDATE') THEN
        NEW.geom := ST_Force3D(NEW.geom);
        draw2d   := ST_Force2D(NEW.geom);

        code_txt     := NEW.code::text;
        comments_txt := CASE WHEN NEW.comments IS NULL THEN NULL ELSE NEW.comments::text END;
        IF TG_OP = 'UPDATE' THEN
            old_code_txt     := OLD.code::text;
            old_comments_txt := CASE WHEN OLD.comments IS NULL THEN NULL ELSE OLD.comments::text END;
        END IF;

        -- Leer snap_distance preferente de metadata (silencioso)
        BEGIN
            SELECT formation_section_snap_distance INTO fs_snap_base FROM _albion.metadata;
        EXCEPTION WHEN undefined_table OR undefined_column THEN
            BEGIN
                SELECT snap_distance INTO fs_snap_base FROM _albion.metadata;
            EXCEPTION WHEN undefined_table OR undefined_column THEN
                NULL;
            END;
        END;
        IF fs_snap_base IS NULL OR fs_snap_base <= 0 THEN
            fs_snap_base := 0.5;
        END IF;
        fs_snap := fs_snap_base;

        ------------------------------------------------------------
        -- 1) Buscar candidato en albion.formation_section
        --    Orden: intersecta (TRUE primero) luego distancia
        ------------------------------------------------------------
        SELECT fs.section_id, fs.hole_id
          INTO fs_section_id, fs_hole_id
        FROM albion.formation_section fs
        ORDER BY
          (ST_Intersects(ST_Force2D(fs.geom), draw2d)) DESC,
          ST_Distance(ST_Force2D(fs.geom), draw2d) ASC
        LIMIT 1;

        IF fs_section_id IS NOT NULL THEN
            fs_found := true;
        END IF;

        ------------------------------------------------------------
        -- 2) Asignar sección / pozo desde formation_section si existe
        ------------------------------------------------------------
        sid := COALESCE(fs_section_id, NEW.section_id);
        hid := COALESCE(fs_hole_id, NEW.hole_id);

        ------------------------------------------------------------
        -- 3) Fallback si NO hubo candidato en formation_section
        ------------------------------------------------------------
        IF NOT fs_found THEN
            -- Determinar sección más cercana (usando startpoint para mantener compat.)
            IF sid IS NULL THEN
                SELECT s.id INTO sid
                FROM _albion.section s
                ORDER BY ST_Distance(s.geom, ST_StartPoint(draw2d)) ASC
                LIMIT 1;
            END IF;

            IF sid IS NULL THEN
                RAISE EXCEPTION 'No se pudo determinar section_id.';
            END IF;

            -- Determinar hole dentro de esa sección
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
        -- 4) Cargar datos de sección y pozo definitivos
        ------------------------------------------------------------
        SELECT anchor, geom, scale INTO a, sg, sc
        FROM _albion.section
        WHERE id = sid;

        SELECT geom, depth_ INTO hs, dpt
        FROM _albion.hole
        WHERE id = hid;

        ------------------------------------------------------------
        -- 5) Calcular from_/to_ sólo si faltan
        ------------------------------------------------------------
        IF NEW.from_ IS NULL OR NEW.to_ IS NULL THEN
            g3d := albion.from_section(draw2d, a, sg, sc);
            t0  := ST_LineLocatePoint(hs, ST_StartPoint(g3d));
            t1  := ST_LineLocatePoint(hs, ST_EndPoint(g3d));
            f0  := LEAST(GREATEST(LEAST(t0,t1) * dpt, 0), dpt);
            f1  := LEAST(GREATEST(GREATEST(t0,t1) * dpt, 0), dpt);
        ELSE
            f0 := NEW.from_;
            f1 := NEW.to_;
        END IF;

        -- Espesor mínimo
        IF f1 - f0 < eps THEN
            f1 := LEAST(f0 + eps, dpt);
            IF f1 - f0 < eps THEN
                f0 := GREATEST(f1 - eps, 0);
            END IF;
        END IF;

        ------------------------------------------------------------
        -- 6) Lookup de comments (solo si vacío / no personalizado)
        ------------------------------------------------------------
        IF TG_OP = 'INSERT' THEN
            IF (comments_txt IS NULL OR trim(comments_txt) = '')
               AND (code_txt IS NOT NULL AND trim(code_txt) <> '') THEN
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
                 OR trim(comments_txt) = ''
                 OR comments_txt = old_comments_txt
               )
               AND (
                    old_comments_txt IS NULL
                 OR trim(old_comments_txt) = ''
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
    -- 8) DML subyacente
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