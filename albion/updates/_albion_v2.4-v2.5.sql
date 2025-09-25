-- This sql script applies all modifications done on the db structure between the 2.4 and 2.5 version.

-- commit b6523057 - Creates the update_consensus_hole function
CREATE OR REPLACE FUNCTION albion.update_consensus_hole()
RETURNS VOID
LANGUAGE plpgsql
AS $$
    BEGIN
        INSERT INTO
            _albion.hole(id, x, y, z, depth_, comments)
        VALUES (
            'CONSENSUS_',
            (SELECT MIN(x) - 0.1 * (MAX(x) - MIN(x)) FROM _albion.hole),
            (SELECT MIN(y) - 0.1 * (MAX(y) - MIN(y)) FROM _albion.hole),
            0,
            (SELECT MAX(depth_) FROM _albion.hole),
            'Consensus hole (visualization artefact)'
        )
        ON CONFLICT (id)
        DO
            UPDATE SET
            	depth_ = (SELECT max(depth_) FROM _albion.hole),
		        x = (SELECT min(x) - 0.1 * (max(x) - min(x)) FROM _albion.hole),
                y = (SELECT min(y) - 0.1 * (max(y) - min(y)) FROM _albion.hole)
        ;
    END;
$$
;

-- commit b6523057 - Split the collar_instead_fct function in 2 ones.
-- collar_instead_fct for the creation and update, and collar_instead_delete_fct for the deletion.
CREATE OR REPLACE function albion.collar_instead_fct()
returns trigger
language plpgsql
as
$$
    begin
        new.date_ := coalesce(new.date_, now()::date::varchar);

        if tg_op = 'INSERT' then
            insert into
                _albion.hole(id, date_, depth_, x, y, z, comments)
            values(
                   new.id, new.date_, new.depth_, st_x(new.geom), st_y(new.geom), st_z(new.geom), new.comments
            )
            returning id into new.id;

        elsif tg_op = 'UPDATE' then
            update
                _albion.hole
            set
                id=new.id, date_=new.date_, depth_=new.depth_, x=st_x(new.geom), y=st_y(new.geom), z=st_z(new.geom), comments=new.comments
            where id=old.id;
        end if;

        -- To be correctly calculated, the deviation linked to this hole must be added. It can only be added after...
        -- Due to constraint
        update _albion.hole set geom = albion.hole_geom(new.id) where id=new.id;

	    PERFORM albion.update_consensus_hole();
        return new;
    end;
$$
;

CREATE OR REPLACE trigger collar_instead_trig
    instead of insert or update on albion.collar
       for each row execute procedure albion.collar_instead_fct()
;

CREATE OR REPLACE function albion.collar_instead_delete_fct()
returns trigger
language plpgsql
as
$$
    begin
        delete from _albion.collar where id=old.id;
	    PERFORM albion.update_consensus_hole();
        return old;
    end;
$$
;

CREATE OR REPLACE trigger collar_instead_delete_trig
    instead of delete on albion.collar
       for each row execute procedure albion.collar_instead_delete_fct()
;

-- commit 4441c8e5 + d0bda95 - Add raw_hole view
CREATE OR REPLACE VIEW albion.raw_hole AS SELECT id, x, y, z, depth_, date_, comments FROM _albion.hole
;

CREATE OR REPLACE VIEW albion.raw_deviation AS SELECT hole_id, from_, dip, azimuth FROM _albion.deviation
;

CREATE OR REPLACE FUNCTION albion.raw_hole_instead_insert_fct()
RETURNS trigger
LANGUAGE plpgsql
AS $$
    BEGIN
        INSERT INTO
            _albion.hole(id, date_, depth_, x, y, z, comments)
        VALUES (
            NEW.id, NEW.date_, NEW.depth_, NEW.x, NEW.y, NEW.z, NEW.comments
        )
        ON CONFLICT (id)
        DO NOTHING
        RETURNING id INTO NEW.id;

        IF EXISTS (SELECT 1 FROM albion.named_section) THEN
            PERFORM albion.snap_section_to_collar(NEW);
        END IF;

        RETURN NEW;
    END;
$$
;

CREATE OR REPLACE TRIGGER raw_hole_instead_insert_trig
    INSTEAD OF INSERT ON albion.raw_hole
        FOR EACH ROW EXECUTE PROCEDURE albion.raw_hole_instead_insert_fct()
;

-- commit d0bda95 + 596d1b3a + a52fbc9d +  b70dbd3f - Creates the snap_section_to_collar function
CREATE OR REPLACE FUNCTION albion.snap_section_to_collar(hole albion.raw_hole)
RETURNS VOID
LANGUAGE plpgsql
AS $$
    DECLARE
        collar_point geometry;
        max_distance REAL;
    BEGIN
        collar_point := ST_SetSRID(ST_Point(hole.x, hole.y), $SRID);
        SELECT max_snapping_distance INTO max_distance FROM albion.metadata;

        UPDATE
            albion.named_section
        SET
            geom = ST_Snap(geom, collar_point, max_distance)
        WHERE id in (
            SELECT
                ns.id
            FROM
                LATERAL (
                    SELECT DISTINCT ON (s.section) s.id, s.geom, s.section, s.geom <-> collar_point AS dist
                    FROM albion.named_section AS s
                    ORDER BY s.section, dist
                    LIMIT ( SELECT count(DISTINCT section) FROM albion.named_section )
                ) AS ns
            WHERE ns.dist < max_distance AND ns.dist != 0
        );
    END;
$$
;

-- commit  596d1b3a - Creates the albion.named_section view
-- for updating section cut at the same time than named section
CREATE OR REPLACE FUNCTION create_section_cut(new albion.named_section)
RETURNS geometry
LANGUAGE plpgsql
as $$
    BEGIN
        RETURN (
            with geom as (
                select st_dumppoints(new.geom) as pt
            ),
            segment as (
                select st_makeline(lag((pt).geom) over (order by (pt).path), (pt).geom) as geom from geom
            ),
            filtered as (
                select geom from segment as s
                except
                select s.geom from segment as s
                join _albion.named_section as o
                on st_intersects(o.cut, s.geom)
                and st_linelocatepoint(s.geom, st_intersection(o.cut, s.geom)) not in (0.0, 1.0)
                and st_geometrytype(st_intersection(o.cut, s.geom)) = 'ST_Point'
            )
            select st_multi(st_linemerge(st_collect(geom))) from filtered
        );
    END;
$$
;

CREATE OR REPLACE function albion.named_section_instead_fct()
returns trigger
language plpgsql
as
$$
    begin
        if tg_op = 'INSERT' then
            new.id := _albion.unique_id()::varchar;
            new.cut := create_section_cut(new);
        elsif tg_op = 'UPDATE' then
            IF NOT ST_Equals(new.geom, old.geom) then
                new.cut := create_section_cut(new);
            END IF;
        end if;

        if tg_op = 'INSERT' then
            insert into _albion.named_section(id, geom, cut, section)
            values(new.id, new.geom, new.cut, new.section)
            returning id into new.id;
            return new;
        elsif tg_op = 'UPDATE' then
            update _albion.named_section set id=new.id, geom=new.geom, cut=new.cut, section=new.section
            where id=old.id;
            return new;
        elsif tg_op = 'DELETE' then
            delete from _albion.named_section where id=old.id;
            return old;
        end if;
    end;
$$
;

CREATE OR REPLACE TRIGGER named_section_instead_trig
    instead of insert or update or delete on albion.named_section
       for each row execute procedure albion.named_section_instead_fct()
;

-- commit b70dbd3f - Add max snapping distance in metadata
ALTER TABLE _albion.metadata
ADD COLUMN IF NOT EXISTS max_snapping_distance real DEFAULT 10
;

CREATE OR REPLACE view albion.metadata
as
    select id, srid, close_collar_distance, snap_distance, precision,
    interpolation, end_node_relative_distance, end_node_relative_thickness,
    correlation_distance, correlation_angle, parent_correlation_angle, max_snapping_distance
from _albion.metadata
;

-- commit 43b3b7f6 - Fix conflict with new added edges
CREATE OR REPLACE function albion.edge_instead_fct()
returns trigger
language plpgsql
as
$$
    declare
        edge_ok integer;
    begin
        if tg_op in ('INSERT', 'UPDATE') then
            new.start_ := coalesce(new.start_, (select id from _albion.node where st_intersects(geom, new.geom) and st_centroid(geom)::varchar=st_startpoint(new.geom)::varchar));
            new.end_ := coalesce(new.end_, (select id from _albion.node where st_intersects(geom, new.geom) and st_centroid(geom)::varchar=st_endpoint(new.geom)::varchar));
            if new.start_ > new.end_ then
                select new.start_, new.end_ into new.end_, new.start_;
            end if;
            -- check that edge is in all_edge
            select count(1)
            from albion.all_edge as ae
            join _albion.hole as hs on hs.id=ae.start_
            join _albion.hole as he on he.id=ae.end_
            join _albion.node as ns on (ns.hole_id in (hs.id, he.id) and ns.id=new.start_)
            join _albion.node as ne on (ne.hole_id in (hs.id, he.id) and ne.id=new.end_)
            into edge_ok;
            if edge_ok = 0 then
                raise EXCEPTION 'impossible edge (not a cell edge)';
            end if;
            new.geom := st_makeline(
                st_3dlineinterpolatepoint((select geom from _albion.node where id=new.start_), .5),
                st_3dlineinterpolatepoint((select geom from _albion.node where id=new.end_), .5));
        end if;

        if tg_op = 'INSERT' then
            insert into _albion.edge(id, start_, end_, graph_id, geom)
            values(new.id, new.start_, new.end_, new.graph_id, new.geom)
            ON CONFLICT (start_, end_) DO NOTHING
            returning id into new.id;
            return new;
        elsif tg_op = 'UPDATE' then
            update _albion.edge set id=new.id, start_=new.start_, end_=new.end_, graph_id=new.graph_id, new._geom=new.geom
            where id=old.id;
            return new;
        elsif tg_op = 'DELETE' then
            delete from _albion.edge where id=old.id;
            return old;
        end if;
    end;
$$
;

CREATE OR REPLACE trigger edge_instead_trig
    instead of insert or update or delete on albion.edge
       for each row execute procedure albion.edge_instead_fct()
;

-- commit e467a6b1 - Avoid overwriting end node when adding new collars
ALTER TABLE _albion.end_node
ADD CONSTRAINT unique_end_node UNIQUE (node_id, hole_id, graph_id)
;

-- commit d33329da - Avoid conflict on sections
CREATE OR REPLACE function albion.section_instead_fct()
returns trigger
language plpgsql
as
$$
    begin
        if tg_op = 'INSERT' then
            insert into _albion.section(id, anchor, geom, scale)
                values(new.id, new.anchor, new.geom, new.scale)
                ON CONFLICT (id) DO UPDATE
                SET id=new.id, anchor=new.anchor, geom=new.geom, scale=new.scale
                returning id, geom into new.id, new.geom;
            return new;
        elsif tg_op = 'UPDATE' then
            update _albion.section set id=new.id, anchor=new.anchor, geom=new.geom, scale=new.scale
            where id=old.id;
            return new;
        elsif tg_op = 'DELETE' then
            delete from _albion.section where id=old.id;
            return old;
        end if;
    end;
$$
;

CREATE OR REPLACE trigger section_instead_trig
    instead of insert or update or delete on albion.section
       for each row execute procedure albion.section_instead_fct()
;

-- commit c729fcc9 + 9be327da + 150a8513 - Modify the offset definition for graph without any parent
CREATE OR REPLACE FUNCTION albion.valid_offset(startnode geometry, endnode geometry, angle real)
RETURNS boolean
LANGUAGE plpgsql
AS
$$
    DECLARE
           start_offset boolean;
           centroid_offset boolean;
           end_offset boolean;
    BEGIN
	   -- start offset
           start_offset := (
	       SELECT abs(
	           st_z(st_startpoint(endnode)) - st_z(st_startpoint(startnode))
	       ) <= tan(angle * pi() / 180) * st_distance(st_startpoint(startnode), st_startpoint(endnode))
	   );
	   -- centroid offset
           centroid_offset := (
	       SELECT abs(
	           (st_z(st_endpoint(startnode)) + st_z(st_startpoint(startnode))) / 2
		   - (st_z(st_endpoint(endnode)) + st_z(st_startpoint(endnode))) / 2
	       ) <= tan(angle * pi() / 180) * st_distance(st_centroid(startnode), st_centroid(endnode))
	   );
	   -- end offset
           end_offset := (
	       SELECT abs(
	           st_z(st_endpoint(endnode)) - st_z(st_endpoint(startnode))
	       ) <= tan(angle * pi() / 180) * st_distance(st_endpoint(startnode), st_endpoint(endnode))
	   );
           RETURN (SELECT start_offset OR centroid_offset OR end_offset);
    END;
$$
;

CREATE OR REPLACE view albion.possible_edge as
-- resulting request: which edges should be drawn for each couple of adjacent nodes in the current graph
with edge_result as (
    select
        ns.id as start_,
	ne.id as end_,
	ns.graph_id as graph_id,
	(
	    st_makeline(
	        st_3dlineinterpolatepoint(ns.geom, .5),
		st_3dlineinterpolatepoint(ne.geom, .5)
	    )
	)::geometry('LINESTRINGZ', 32632) as geom

    from
        albion.all_edge as e
    join
        _albion.hole as hs on hs.id=e.start_
    join
        _albion.hole as he on he.id=e.end_
    join
        _albion.node as ns on ns.hole_id=hs.id
    join
        _albion.node as ne on ne.hole_id=he.id
    join
        _albion.graph as g on g.id=ns.graph_id
    , albion.metadata as md

    where
        ns.graph_id = ne.graph_id
    and
    (
	-- no parent, check the regular offset
	-- the absolute value of current offset should be smaller than the regular allowed offset
	-- |current_offset| <= allowed_offset_regular
	(
	    ns.parent is null
	    and
	    albion.valid_offset(ns.geom, ne.geom, md.correlation_angle)
	)
	or
	-- there is a parent, check the parent offset in addition to the regular offset
	-- the absolute value of current offset minus the parent offset should be smaller than the allowed offset for childs
	-- |current_offset - parent_offset| <= allowed_offset_with_parent
	--
	-- the offset is evaluated regarding start point, median point and end point... It is valid
	-- if at least one of these three cases is valid.
	--
	-- TODO: make this part of the query brighter in SQL (the query is already awful, and this part is even worst)
	-- Try to consider albion.valid_offset function?
	(
	    ns.parent is not null
	    and ne.parent is not null
	    and
	    (
	    -- Valid offset when considering start points?
	    (
		abs(
		    (st_z(st_startpoint(ne.geom)) - st_z(st_startpoint(ns.geom)))
		    - coalesce(
		        (
			-- the parent offset is kind of hard to extract...
			-- one considers the possible edges that start from the start node parent
			-- the reference parent edge is set by the closest ending node (from the current end node), in the parent graph
			select st_z(st_startpoint(nre.geom)) - st_z(st_startpoint(nrs.geom))
			from albion.all_edge as e
			join _albion.hole as hs on hs.id=e.start_
			join _albion.hole as he on he.id=e.end_
			join _albion.node as nrs on nrs.hole_id=hs.id
			join _albion.node as nre on nre.hole_id=he.id
			where nre.graph_id = nrs.graph_id
			and nrs.id = ns.parent
			and nre.id = ne.parent
			-- be careful: the parent edge should be a possible edge itself
			-- TODO: manage the recursivity in the model...
			and albion.valid_offset(nrs.geom, nre.geom, md.correlation_angle)
			order by abs(st_z(st_startpoint(nre.geom)) - st_z(st_startpoint(ne.geom)))
			limit 1
			-- if it does not have any parent, consider a large value so as to unvalidate the possible edge
			), '+infinity'
		    )
		)
		<= tan(md.parent_correlation_angle * pi() / 180) * st_distance(st_startpoint(ns.geom), st_startpoint(ne.geom))
	    )
	    or
	    -- Valid offset when considering median points?
	    (
		abs(
			(
				(st_z(st_endpoint(ne.geom)) + st_z(st_startpoint(ne.geom))) / 2
				- (st_z(st_endpoint(ns.geom)) + st_z(st_startpoint(ns.geom))) / 2
			)
		    - coalesce(
		        (
			-- the parent offset is kind of hard to extract...
			-- one considers the possible edges that start from the start node parent
			-- the reference parent edge is set by the closest ending node (from the current end node), in the parent graph
			select (
			       (st_z(st_endpoint(nre.geom)) + st_z(st_startpoint(nre.geom))) / 2
			       - (st_z(st_endpoint(nrs.geom)) + st_z(st_startpoint(nrs.geom))) / 2
			)
			from albion.all_edge as e
			join _albion.hole as hs on hs.id=e.start_
			join _albion.hole as he on he.id=e.end_
			join _albion.node as nrs on nrs.hole_id=hs.id
			join _albion.node as nre on nre.hole_id=he.id
			where nre.graph_id = nrs.graph_id
			and nrs.id = ns.parent
			and nre.id = ne.parent
			-- be careful: the parent edge should be a possible edge itself
			-- TODO: manage the recursivity in the model...
			and albion.valid_offset(nrs.geom, nre.geom, md.correlation_angle)
			order by abs(
			      (st_z(st_startpoint(nre.geom)) + st_z(st_endpoint(nre.geom))) / 2
			      - (st_z(st_startpoint(ne.geom)) + st_z(st_endpoint(ne.geom))) / 2
			)
			limit 1
			-- if it does not have any parent, consider a large value so as to unvalidate the possible edge
			), '+infinity'
		    )
		)
		<= tan(md.parent_correlation_angle * pi() / 180) * st_distance(st_centroid(ns.geom), st_centroid(ne.geom))
	    )
	    or
	    -- Valid offset when considering end points?
	    (
		abs(
		    (st_z(st_endpoint(ne.geom)) - st_z(st_endpoint(ns.geom)))
		    - coalesce(
		        (
			-- the parent offset is kind of hard to extract...
			-- one considers the possible edges that start from the end node parent
			-- the reference parent edge is set by the closest ending node (from the current end node), in the parent graph
			select st_z(st_endpoint(nre.geom)) - st_z(st_endpoint(nrs.geom))
			from albion.all_edge as e
			join _albion.hole as hs on hs.id=e.start_
			join _albion.hole as he on he.id=e.end_
			join _albion.node as nrs on nrs.hole_id=hs.id
			join _albion.node as nre on nre.hole_id=he.id
			where nre.graph_id = nrs.graph_id
			and nrs.id = ns.parent
			and nre.id = ne.parent
			-- be careful: the parent edge should be a possible edge itself
			-- TODO: manage the recursivity in the model...
			and albion.valid_offset(nrs.geom, nre.geom, md.correlation_angle)
			order by abs(st_z(st_endpoint(nre.geom)) - st_z(st_endpoint(ne.geom)))
			limit 1
			-- if it does not have any parent, consider a large value so as to unvalidate the possible edge
			), '+infinity'
		    )
		)
		<= tan(md.parent_correlation_angle * pi() / 180) * st_distance(st_endpoint(ns.geom), st_endpoint(ne.geom))
	    )
	    )
	)
    )
)
select row_number() over() as id, * from edge_result
;

-- commit d3f1c9c6 - Drop unused extension
DROP EXTENSION IF EXISTS postgis_sfcgal
;

-- commit d3f1c9c6 - Drop unused extension
CREATE OR REPLACE function albion.tesselate(polygon_ geometry, lines_ geometry, points_ geometry)
returns geometry
language plpython3u volatile
as
$$
    from shapely import wkb
    from shapely import geos
    geos.WKBWriter.defaults['include_srid'] = True
    from fourmy import tessellate

    polygon = wkb.loads(bytes.fromhex(polygon_))
    lines = wkb.loads(bytes.fromhex(lines_)) if lines_ else None
    points = wkb.loads(bytes.fromhex(points_)) if points_ else None
    result = tessellate(polygon, lines, points)

    geos.lgeos.GEOSSetSRID(result._geom, geos.lgeos.GEOSGetSRID(polygon._geom))
    return result.wkb_hex
$$
;
