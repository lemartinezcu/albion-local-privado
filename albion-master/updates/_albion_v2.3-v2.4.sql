-- add a type column, for characterizing graphs
alter table _albion.graph add column graph_type text
;

-- modify the architecture of the database for considering parentship
-- that a one-to-many relationship, one drops the parent field in graph table...
alter table _albion.graph drop column parent cascade
;

-- ...and subsequently adds a dedicated table
create table _albion.graph_relationship(
    parent_id varchar references _albion.graph(id) on delete cascade on update cascade,
    child_id varchar references _albion.graph(id) on delete cascade on update cascade
)
;

-- modification of the graph view
create or replace view albion.graph as
select id, graph_type from _albion.graph
;

-- creation of the graph relationship view
create or replace view albion.graph_relationship as
select parent_id, child_id from _albion.graph_relationship
;

/*
 * Find a new parent for a node that is on a given hole, in a given child graph,
 * with a given geometry. One looks for the closest node belonging to a parent of the child graph,
 * with respect to the Z-axis, on the same collar.
 */
create or replace function albion.find_new_parent(new_hole_id text, new_graph_id text, new_geom geometry)
returns text
as $$
declare
	result_ text;
begin
select n.id as parent_id
into result_
from albion.node as n
join albion.graph_relationship as gr
on n.graph_id = gr.parent_id  -- the node one looks for is on a parent graph
where n.hole_id = new_hole_id  -- on the same collar
and gr.child_id = new_graph_id  -- it is necessarily considered as a child
-- one focuses on distance over the Z-axis
order by abs(st_z(st_startpoint(n.geom)) - st_z(st_startpoint(new_geom)))
limit 1;
return result_;
end;
$$ language plpgsql
;

/*
 * Update the nodes of the given graph childs:
 * by adding a new parent graph, one has to verify (and update) the 'parent' column
 * for every node that belongs to a child graph.
 */
create or replace function albion.update_child_node_fn(new_graph_id text)
returns void as $$
begin
-- get the node of the current graph childs
with child_node as (
	select *
	from albion.node as nc
	join albion.graph_relationship as gr
	on nc.graph_id=gr.child_id
	where gr.parent_id=new_graph_id
),
-- get the node of the parent of these child graphs
parent_node as (
	select *
	from albion.node as np
	join albion.graph_relationship as gr
	on np.graph_id=gr.parent_id
	where gr.child_id in (select graph_id from child_node)
)
-- the new reference parent is the closest from each node in the child graphs
UPDATE albion.node AS n SET parent=child_update.id FROM (
	SELECT DISTINCT ON (cn.id) pn.id, cn.id as child_id
	FROM child_node AS cn, parent_node as pn
	ORDER BY cn.id, ABS(st_z(st_startpoint(cn.geom)) - st_z(st_startpoint(pn.geom)))
) AS child_update
WHERE n.id = child_update.child_id;
end;
$$ language plpgsql
;

/*
 * Inserting a node in a graph requires to automatically determine its parent before the insertion
 * operation, if the node belongs to a child graph. After the insertion operation, the nodes that
 * belongs to the child of the current graph are updated as well, as the insertion may have
 * introduced closer parent nodes.
 */
create or replace function albion.node_insert_fn()
returns trigger as
$$
	begin
    new.geom := coalesce(new.geom, albion.hole_piece(new.from_, new.to_, new.hole_id));
	-- if we try to insert/update a node from a graph that has at least one parent
	-- the parent node id is set from the parent graphs
	if (select count(*) from albion.graph_relationship where child_id=new.graph_id) > 0 then
		new.parent := (select albion.find_new_parent(new.hole_id, new.graph_id, new.geom));
	end if;
	-- insert new node in _albion.node table
	insert into _albion.node(id, graph_id, hole_id, from_, to_, geom, parent)
	values(new.id, new.graph_id, new.hole_id, new.from_, new.to_, new.geom, new.parent)
	returning id into new.id;
	-- if we try to insert/update a node from a graph that is itself a parent from other graphs
	-- one needs to check if the 'parent' value for child nodes should be updated
	if (select count(*) from albion.graph_relationship where parent_id=new.graph_id) > 0 then
		perform albion.update_child_node_fn(new.graph_id);
	end if;
	return new;
    end;
$$ language plpgsql
;

drop trigger if exists node_insert_trig on albion.node
;

create trigger node_insert_trig instead of insert on albion.node
for each row execute procedure albion.node_insert_fn()
;

/*
 * Updating a node in a graph requires to compute the new parent (the node geometry may have
 * changed). Additionally, one updates every node from the node graph childs.
 */
create or replace function albion.node_update_fn()
returns trigger as
$$
    begin
        new.geom := coalesce(new.geom, albion.hole_piece(new.from_, new.to_, new.hole_id));
	-- if we try to insert/update a node from a graph that has at least one parent
	-- the parent node id is set from the parent graphs
	if (select count(*) from albion.graph_relationship where child_id=new.graph_id) > 0 then
	    new.parent := (select albion.find_new_parent(new.hole_id, new.graph_id, new.geom));
	end if;
	-- update the node
	update _albion.node
	set
	    id=new.id,
	    graph_id=new.graph_id,
	    hole_id=new.hole_id,
	    from_=new.from_,
	    to_=new.to_,
	    geom=new.geom,
	    parent=new.parent
	where id=old.id;
	return new;
	-- if we try to insert/update a node from a graph that is itself a parent from other graphs
	-- one needs to check if the 'parent' value for child nodes should be updated
	if (select count(*) from albion.graph_relationship where parent_id=new.graph_id) > 0 then
	    perform albion.update_child_node_fn(new.graph_id);
	end if;
	return new;
    end;
$$ language plpgsql
;

drop trigger if exists node_update_trig on albion.node
;

create trigger node_update_trig instead of update on albion.node
for each row execute procedure albion.node_update_fn()
;

/*
 * Deleting a node causes the update of the nodes that belongs to child graph (the deleted node may
 * be itself the parent of other existing nodes).
 */
create or replace function albion.node_delete_fn()
returns trigger as
$$
    begin
	-- delete the node
        delete from _albion.node where id=old.id;
	-- if we try to insert/update a node from a graph that is itself a parent from other graphs
	-- one needs to check if the 'parent' value for child nodes should be updated
	if (select count(*) from albion.graph_relationship where parent_id=old.graph_id) > 0 then
	    perform albion.update_child_node_fn(old.graph_id);
	end if;
	return old;
    end;
$$ language plpgsql
;

drop trigger if exists node_delete_trig on albion.node
;

create trigger node_delete_trig instead of delete on albion.node
for each row execute procedure albion.node_delete_fn()
;

/*
 * The possible edge are drawn with respect to a correlation angle, that defines the allowed offset
 * between starting and ending nodes. If the current graph has a parent, the possible edge depends
 * on a specific correlation angle and on the parent offset between the starting and ending nodes in
 * the parent graph.
*/
create or replace view albion.possible_edge as
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
	    abs(st_z(st_startpoint(ne.geom)) - st_z(st_startpoint(ns.geom))) <= tan(md.correlation_angle * pi() / 180) * st_distance(st_startpoint(ns.geom), st_startpoint(ne.geom))
	)
	or
	-- there is a parent, check the parent offset in addition to the regular offset
	-- the absolute value of current offset plus parent offset should be smaller than the allowed offset for childs
	-- |current_offset + parent_offset| <= allowed_offset_with_parent
	(
	    ns.parent is not null
	    and
	    (
		abs(
		    (st_z(st_startpoint(ne.geom)) - st_z(st_startpoint(ns.geom)))
		    - (
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
			-- be careful: the parent edge should be a possible edge itself
			-- one checks it as if it does not have any parent
			-- TODO: manage the recursivity in the model...
			and abs(st_z(st_startpoint(nre.geom)) - st_z(st_startpoint(nrs.geom))) <= tan(md.correlation_angle * pi() / 180) * st_distance(st_startpoint(nrs.geom), st_startpoint(nre.geom))
			order by abs(st_z(st_startpoint(nre.geom)) - st_z(st_startpoint(ne.geom)))
			limit 1
		    )
		)
		<= tan(md.parent_correlation_angle * pi() / 180) * st_distance(st_startpoint(ns.geom), st_startpoint(ne.geom))
	    )
	)
    )
)
select row_number() over() as id, * from edge_result
;


/*
 * Update the collar_instead_fct, as there was a bug related to consensus collar in it
 */
create or replace function albion.collar_instead_fct()
returns trigger
language plpgsql
as
$$
    begin
        if tg_op in ('INSERT', 'UPDATE') then
            new.date_ := coalesce(new.date_, now()::date::varchar);
        end if;

        if tg_op = 'INSERT' then
            insert into _albion.hole(id, date_, depth_, x, y, z, comments)
            values(new.id, new.date_, new.depth_, st_x(new.geom), st_y(new.geom), st_z(new.geom), new.comments)
            returning id into new.id;
            update _albion.hole set geom = albion.hole_geom(new.id) where id=new.id;
	    update _albion.hole set
	           depth_ = (select max(depth_) from _albion.hole),
		   x = (select min(x) - 0.1 * (max(x) - min(x)) from _albion.hole),
		   y = (select min(y) - 0.1 * (max(y) - min(y)) from _albion.hole),
		   z = 0
		   where id='CONSENSUS_'
	    ;
            return new;
        elsif tg_op = 'UPDATE' then
            update _albion.hole set id=new.id, date_=new.date_, depth_=new.depth_, x=st_x(new.geom), y=st_y(new.geom), z=st_z(new.geom), comments=new.comments
            where id=old.id;
            update _albion.hole set geom = albion.hole_geom(new.id) where id=new.id;
	    update _albion.hole set
	           depth_ = (select max(depth_) from _albion.hole),
		   x = (select min(x) - 0.1 * (max(x) - min(x)) from _albion.hole),
		   y = (select min(y) - 0.1 * (max(y) - min(y)) from _albion.hole),
		   z = 0
		   where id='CONSENSUS_'
	    ;
            return new;
        elsif tg_op = 'DELETE' then
            delete from _albion.collar where id=old.id;
	    update _albion.hole set
	           depth_ = (select max(depth_) from _albion.hole),
		   x = (select min(x) - 0.1 * (max(x) - min(x)) from _albion.hole),
		   y = (select min(y) - 0.1 * (max(y) - min(y)) from _albion.hole),
		   z = 0
		   where id='CONSENSUS_'
	    ;
            return old;
        end if;
    end;
$$
;

/*
 * Update the volume_section view, as there was a non-generic SRID instead of '$SRID'
 */
create or replace view albion.volume_section as
select se.section_id, ef.graph_id, st_collectionhomogenize(st_collect(ef.geom))::geometry('MULTIPOLYGONZ', $SRID) as geom
from albion.section_edge as se
join albion.edge_face as ef on ef.start_ = se.start_ and ef.end_ = se.end_ and not st_isempty(ef.geom)
group by se.section_id, ef.graph_id
;
