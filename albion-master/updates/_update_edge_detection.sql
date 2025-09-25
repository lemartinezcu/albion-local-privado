create or replace function albion.edge_section_instead_fct()
returns trigger
language plpgsql
as
$$
    declare
        new_geom geometry;
    begin
        if tg_op in ('INSERT', 'UPDATE') then
            new.start_ := coalesce(new.start_, (select node_id from albion.node_section as n, _albion.metadata as m
                    where st_dwithin(n.geom, st_startpoint(new.geom), m.snap_distance)
                    and graph_id=new.graph_id
                    order by st_distance(n.geom, st_startpoint(new.geom)) asc
                    limit 1
                    ));
            new.end_ := coalesce(new.end_, (select node_id from albion.node_section as n, _albion.metadata as m
                    where st_dwithin(n.geom, st_endpoint(new.geom), m.snap_distance)
                    and graph_id=new.graph_id
                    order by st_distance(n.geom, st_endpoint(new.geom)) asc
                    limit 1
                    ));
            if new.start_ > new.end_ then
                select new.start_, new.end_ into new.end_, new.start_;
                select st_reverse(new.geom) into new.geom;
            end if;
            select st_makeline(st_3dlineinterpolatepoint(s.geom, .5), st_3dlineinterpolatepoint(e.geom, .5))
            from _albion.node as s, _albion.node as e
            where s.id=new.start_ and e.id=new.end_ into new_geom;

            -- test if edge is possible
            if not exists (select 1
                            from albion.all_edge as ae
                            join _albion.node as ns on ae.start_ = ns.hole_id
                            join _albion.node as ne on ae.end_ = ne.hole_id
                            where new.start_ = least(ns.id, ne.id) and new.end_=greatest(ns.id, ne.id)) then
                            raise 'edge imposible (%, %)', new.start_, new.end_;
            end if;

        end if;

        if tg_op = 'INSERT' then
            insert into _albion.edge(start_, end_, graph_id, geom)
            values(new.start_, new.end_, new.graph_id, new_geom)
            returning id into new.edge_id;
            return new;
        elsif tg_op = 'UPDATE' then
            update _albion.edge set id=new.edge_id, start_=new.start_, end_=new.end_, graph_id=new.graph_id, new._geom=new_geom
            where id=old.edge_id;
            return new;
        elsif tg_op = 'DELETE' then
            delete from _albion.edge where id=old.edge_id;
            return old;
        end if;
    end;
$$
;

