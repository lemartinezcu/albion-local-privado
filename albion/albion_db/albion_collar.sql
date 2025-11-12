create schema albion
;

-------------------------------------------------------------------------------
-- VIEWS
-------------------------------------------------------------------------------

create view albion.hole as select id, depth_, geom::geometry('LINESTRINGZ', $SRID) from _albion.hole
;

CREATE OR REPLACE VIEW albion.raw_hole AS SELECT id, x, y, z, depth_, date_, comments FROM _albion.hole
;
CREATE OR REPLACE VIEW albion.raw_deviation AS SELECT hole_id, from_, dip, azimuth FROM _albion.deviation
;

create or replace view albion.collar as
    select id, st_startpoint(geom)::geometry('POINTZ', $SRID) as geom, date_, comments, depth_
    from _albion.hole
    where id!='CONSENSUS_'
;
alter view albion.collar alter id set default _albion.unique_id()::varchar
;

-- Consensus collar, a dummy station on which the resistivity consensus is stored
-- By default, one takes the min X and Y and the max Z to localize the point
create or replace view albion.collar_consensus as
    select id, st_startpoint(geom)::geometry('POINTZ', $SRID) as geom, date_, comments, depth_
    from _albion.hole
    where id='CONSENSUS_'
;

-------------------------------------------------------------------------------
-- TRIGGERS
-------------------------------------------------------------------------------

create or replace function albion.collar_instead_fct()
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

create trigger collar_instead_trig
    instead of insert or update on albion.collar
       for each row execute procedure albion.collar_instead_fct()
;



create or replace function albion.collar_instead_delete_fct()
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

create trigger collar_instead_delete_trig
    instead of delete on albion.collar
       for each row execute procedure albion.collar_instead_delete_fct()
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

CREATE TRIGGER raw_hole_instead_insert_trig
    INSTEAD OF INSERT ON albion.raw_hole
        FOR EACH ROW EXECUTE PROCEDURE albion.raw_hole_instead_insert_fct()
;

-------------------------------------------------------------------------------
-- UTILITY FUNCTIONS
-------------------------------------------------------------------------------

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

create or replace function albion.hole_geom(hole_id_ varchar)
returns geometry
language plpgsql stable
as
$$
    declare
        depth_max_ real;
        hole_geom_ geometry;
        x_ double precision;
        y_ double precision;
        z_ double precision;
        collar_geom_ geometry;
        path_ varchar;
    begin

        select x, y, z, depth_ from _albion.hole where id=hole_id_ into x_, y_, z_, depth_max_;
        collar_geom_ := st_setsrid(st_makepoint(x_, y_, z_), $SRID);
        with dz as (
            select
                from_ as md2, coalesce(lag(from_) over w, 0) as md1,
                (dip + 90)*pi()/180 as wd2,  coalesce(lag((dip+90)*pi()/180) over w, 0) as wd1,
                azimuth*pi()/180 as haz2,  coalesce(lag(azimuth*pi()/180) over w, 0) as haz1
            from _albion.deviation
            where azimuth >= 0 and azimuth <=360 and dip < 0 and dip > -180
            and hole_id=hole_id_
            window w AS (order by from_)
        ),
        pt as (
            select md2, wd2, haz2,
            x_ + sum(0.5 * (md2 - md1) * (sin(wd1) * sin(haz1) + sin(wd2) * sin(haz2))) over w as x,
            y_ + sum(0.5 * (md2 - md1) * (sin(wd1) * cos(haz1) + sin(wd2) * cos(haz2))) over w as y,
            z_ - sum(0.5 * (md2 - md1) * (cos(wd2) + cos(wd1))) over w as z
            from dz
            window w AS (order by md1)
        ),
        line as (
            select st_makeline(('SRID=$SRID; POINTZ('||x||' '||y||' '||z||')')::geometry order by md2 asc) as geom
            from pt
        )
        select ST_RemoveRepeatedPoints(st_addpoint(geom, collar_geom_, 0), 1.e-6)
            from line as l
        into hole_geom_;

        if hole_geom_ is not null and st_3dlength(hole_geom_) < depth_max_ and st_3dlength(hole_geom_) > 0 then
            path_ := 'too short';
            -- holes is not long enough
            with last_segment as (
                select st_pointn(hole_geom_, st_numpoints(hole_geom_)-1) as start_, st_endpoint(hole_geom_) as end_
            ),
            direction as (
                select
                (st_x(end_) - st_x(start_))/st_3ddistance(end_, start_) as x,
                (st_y(end_) - st_y(start_))/st_3ddistance(end_, start_) as y,
                (st_z(end_) - st_z(start_))/st_3ddistance(end_, start_) as z
                from last_segment
            )
            select st_addpoint(hole_geom_,
                        st_makepoint(
                            st_x(s.end_) + (depth_max_-st_3dlength(hole_geom_))*d.x,
                            st_y(s.end_) + (depth_max_-st_3dlength(hole_geom_))*d.y,
                            st_z(s.end_) + (depth_max_-st_3dlength(hole_geom_))*d.z
                        ))
            from direction as d, last_segment as s
            into hole_geom_;

            -- hole have no deviation
        elsif hole_geom_ is null or st_3dlength(hole_geom_) = 0 then
            path_ := 'no length';
            select st_makeline( collar_geom_, st_translate(collar_geom_, 0, 0, -depth_max_)) into hole_geom_;
        end if;

        if abs(st_3dlength(hole_geom_) - depth_max_) > 1e-3 then
            raise 'hole %s %s %s %',  hole_id_, depth_max_, st_3dlength(hole_geom_), path_;
        end if;
        return hole_geom_;
    end;
$$
;

create or replace function albion.hole_piece(from_ real, to_ real, hole_id_ varchar)
returns geometry
language plpgsql stable
as
$$
    begin
        return (
            select st_makeline(
                st_3dlineinterpolatepoint(geom, least(from_/l, 1)),
                st_3dlineinterpolatepoint(geom, least(to_/l, 1)))
            from (select geom, st_3dlength(geom) as l from albion.hole where id=hole_id_) as t
        );
    end;
$$
;
