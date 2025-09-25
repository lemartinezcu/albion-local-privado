"""Unit tests for possible edge generation.

Run with "PYTHONPATH=.. pytest"

Needs the definition of a "albion_test_graph" service into the PostgreSQL service
configuration file.

"""

import numpy as np
from psycopg2 import sql
import pytest

from albion.project import Project


def test_triangulate(project_with_data, project_name):
    """Test the triangulation function on a very basic test case
    """
    with project_with_data.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT id, a, b, c FROM albion.cell;")
        res = cur.fetchall()
        assert len(res) == 2
        assert res[0] == ("1", "2", "0", "1")
        assert res[1] == ("2", "2", "1", "3")
    # Now, the function only refreshes some materialized views, the cell amount stays the same.
    project_with_data.triangulate()
    with project_with_data.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT id, a, b, c FROM albion.cell;")
        res = cur.fetchall()
        assert len(res) == 2


def test_single_graph_creation(project_with_data, graph_name, sub_graph_name):
    """Test the creation and the deletion of a single graph."""
    # Creates the graph
    project_with_data.new_graph(graph_name)
    assert project_with_data.get_existing_graphs() == [graph_name]
    with project_with_data.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT * FROM albion.graph;")
        res = cur.fetchall()
        assert res == [(graph_name, None)]

    # Deletes it
    project_with_data.delete_graph(graph_name)
    assert project_with_data.get_existing_graphs() == []
    with project_with_data.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT * FROM albion.graph;")
        res = cur.fetchall()
        assert res == []

    # Recreates it
    project_with_data.new_graph(graph_name)
    assert project_with_data.get_existing_graphs() == [graph_name]
    with project_with_data.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT * FROM albion.graph;")
        res = cur.fetchall()
        assert res == [(graph_name, None)]

    # Tries to duplicate it
    project_with_data.new_graph(graph_name)
    assert project_with_data.get_existing_graphs() == [graph_name]
    with project_with_data.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT * FROM albion.graph;")
        res = cur.fetchall()
        assert res == [(graph_name, None)]


def test_graph_with_parent_creation(project_with_data, graph_name, sub_graph_name):
    """Test the creation of two graphs, one being the parent of the other one. Then test the deletion
    of the parent graph: the child graph must persist, not the relationship.

    """
    project_with_data.new_graph(graph_name)
    project_with_data.new_graph(sub_graph_name, references=[graph_name])

    with project_with_data.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT * FROM albion.graph;")
        res = cur.fetchall()
        assert res == [(graph_name, None), (sub_graph_name, None)]
        cur.execute("SELECT * FROM albion.graph_relationship;")
        res = cur.fetchall()
        assert res == [(graph_name, sub_graph_name)]

    project_with_data.delete_graph(graph_name)

    with project_with_data.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT * FROM albion.graph;")
        res = cur.fetchall()
        assert res == [(sub_graph_name, None)]
        cur.execute("SELECT * FROM albion.graph_relationship;")
        res = cur.fetchall()
        assert res == []

    project_with_data.delete_graph(sub_graph_name)


def test_nodes(project_with_data, graph_name, sub_graph_name, reference_nodes, child_nodes):
    """Test node creation, update, deletion

    * Test 1: create nodes in a simple graph
    * Test 2: create nodes in a child graph, the 'parent' values must refer to 'reference'
    * Test 3: delete a node from child graph: easy operation, one child less
    * Test 4: delete a node from parent graph: the 'parent' values must be updated
    """
    project_with_data.new_graph(graph_name)
    # Test the creation of nodes in a simple graph
    project_with_data.add_to_graph_node(graph_name, reference_nodes)
    with project_with_data.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT graph_id, hole_id, from_, to_, parent FROM albion.node")
        res = cur.fetchall()
        assert res == [
            (graph_name, "0", 20.0, 21.0, None),
            (graph_name, "1", 10.0, 11.0, None),
            (graph_name, "1", 40.0, 41.0, None),
        ]

    # Test the creation of nodes in a child graph
    project_with_data.new_graph(sub_graph_name, references=[graph_name])
    project_with_data.add_to_graph_node(sub_graph_name, child_nodes)
    with project_with_data.connect() as con:
        cur = con.cursor()
        cur.execute(
            sql.SQL(
                "SELECT hole_id, id, from_ FROM albion.node WHERE graph_id={};"
            ).format(sql.Literal(graph_name))
        )
        parent_nodes = cur.fetchall()
        cur.execute(
            sql.SQL(
                "SELECT hole_id, from_, to_, parent FROM albion.node WHERE graph_id={};"
            ).format(sql.Literal(sub_graph_name))
        )
        child_nodes = cur.fetchall()
        expected_parent_node_id_0 = parent_nodes[
            np.argmin([40.0 - pn[2] if pn[0] == "0" else 999.0 for pn in parent_nodes])
        ][1]
        expected_parent_node_id_1 = parent_nodes[
            np.argmin([50.0 - pn[2] if pn[0] == "1" else 999.0 for pn in parent_nodes])
        ][1]
        assert child_nodes == [
            ("0", 40.0, 41.0, expected_parent_node_id_0),
            ("1", 50.0, 51.0, expected_parent_node_id_1),
        ]
    # Let's add another node to parent graph: the child nodes should have evolved
    project_with_data.add_to_graph_node(
        graph_name, [{"from_": 48, "to_": 49, "hole_id": 1}]
    )
    with project_with_data.connect() as con:
        cur = con.cursor()
        cur.execute(
            sql.SQL(
                "SELECT id FROM albion.node WHERE graph_id={} AND hole_id='1' AND from_=48;"
            ).format(sql.Literal(graph_name))
        )
        parent_node = cur.fetchone()[0]
        cur.execute(
            sql.SQL(
                "SELECT hole_id, from_, to_, parent "
                "FROM albion.node WHERE graph_id={} AND hole_id='1';"
            ).format(sql.Literal(sub_graph_name))
        )
        child_nodes = cur.fetchall()
        assert child_nodes == [("1", 50.0, 51.0, parent_node)]
        cur.execute("select id, hole_id, from_, to_, parent, graph_id from albion.node")

    # Let's remove a child node
    with project_with_data.connect() as con:
        cur = con.cursor()
        cur.execute(
            "DELETE FROM albion.node WHERE graph_id=%s AND hole_id=%s;",
            (sub_graph_name, "0"),
        )
        cur.execute(
            sql.SQL(
                "SELECT hole_id, from_, to_, parent FROM albion.node WHERE graph_id={};"
            ).format(sql.Literal(sub_graph_name))
        )
        child_nodes = cur.fetchall()
        assert child_nodes == [("1", 50.0, 51.0, parent_node)]

    # Let's remove a parent node (the child node should be updated)
    with project_with_data.connect() as con:
        cur = con.cursor()
        cur.execute(
            sql.SQL(
                "SELECT id FROM albion.node WHERE graph_id={} AND hole_id='1' AND from_=40;"
            ).format(sql.Literal(graph_name))
        )
        parent_node = cur.fetchone()[0]
        cur.execute(
            "DELETE FROM albion.node WHERE graph_id=%s AND from_=%s;",
            (graph_name, 48),
        )
        cur.execute(
            sql.SQL(
                "SELECT hole_id, from_, to_, parent FROM albion.node WHERE graph_id={};"
            ).format(sql.Literal(sub_graph_name))
        )
        child_nodes = cur.fetchall()
        assert child_nodes == [("1", 50.0, 51.0, parent_node)]


def test_offset_validity(project_with_data, offset_nodes):
    """Test the offset validity function. The offset between two nodes is valid (hence a candidate edge
    should be drawn) if and only if it is under a specific threshold.

    The threshold is expressed as an angle, and the offset is retrieved through the tangent
    function and the distance between the two nodes of interest.

    Three offset are actually checked, respectively regarding the starting point, the median point
    and the ending point within the nodes (which are simple linestrings). The offset is considered
    as valid if there is at least one of these three offsets which is OK.

    In the example one gets a 30-31 node on the first hole and a 20-35 node on the second hole. The
    threshold angles are respectively 5.71, 1.72 and 2.29° for starting, median and ending
    points. Then the offset is valid if the provided threshold is larger than 1.72°.

    """
    # Add a graph
    parent_graph_name = "reference"
    project_with_data.new_graph(parent_graph_name)
    # Add offset nodes (the possible edge creation is implicitly managed through a view
    project_with_data.add_to_graph_node(parent_graph_name, offset_nodes)
    with project_with_data.connect() as con:
        # Case 1: fail on the starting point, but the offset is OK for other points; then valid
        cur = con.cursor()
        cur.execute(
            """
            SELECT albion.valid_offset(
            (SELECT geom FROM albion.node WHERE id='3'),
            (SELECT geom FROM albion.node WHERE id='4'),
            5
            );
            """
        )
        assert cur.fetchone()[0]
        # Case 2: fail on every point; then unvalid
        cur.execute(
            """
            SELECT albion.valid_offset(
            (SELECT geom FROM albion.node WHERE id='3'),
            (SELECT geom FROM albion.node WHERE id='4'),
            1
            );
            """
        )
        assert not cur.fetchone()[0]


def test_add_to_graph_node_reference(project_with_data, graph_name, reference_nodes):
    """Test the possible edge generation, on a first graph, depending on the correlation_angle.

    The input data is as follows:
    * Parent graph : A(z=-20) on h0, B(z=-10) and C(z=-40) on h1

    The expected results are:
    * Test 1: a=5°: no possible edge
    * Test 2: a=10°: one possible edge AB
    * Test 3: a=20°: two possible edges (AB and AC)

    """
    # Add a graph
    project_with_data.new_graph(graph_name)
    # Add references nodes (the possible edge creation is implicitly managed through a view
    project_with_data.add_to_graph_node(graph_name, reference_nodes)
    with project_with_data.connect() as con:
        cur = con.cursor()
        # First test, with default metadata: no possible edge
        cur.execute("SELECT start_, end_, graph_id FROM albion.possible_edge;")
        res = cur.fetchall()
        assert len(res) == 0
        # Second test, with a larger correlation angle: one possible edge
        cur.execute("UPDATE albion.metadata SET correlation_angle=10;")
        cur.execute("SELECT start_, end_, graph_id FROM albion.possible_edge;")
        res = cur.fetchall()
        assert len(res) == 1
        assert res == [("3", "4", graph_name)]
        # Third test, with an even larger correlation angle: two possible edges
        cur.execute("UPDATE albion.metadata SET correlation_angle=20;")
        cur.execute("SELECT start_, end_, graph_id FROM albion.possible_edge;")
        res = cur.fetchall()
        assert len(res) == 2
        assert set(res) == {
            ("3", "4", graph_name),
            ("3", "5", graph_name),
        }


def test_add_to_graph_node_child_with_ref(project_with_data, graph_name, sub_graph_name, reference_nodes, child_nodes):
    """Test the possible edge generation, on a second graph, which refers to the first one, depending
    on the correlation_angle and on the reference correlation angle.

    The input data is as follows:
    * Parent graph : A(z=-20) on h0, B(z=-10) and C(z=-40) on h1
    * Child graph : D(z=-40) on h0, E(z=-50) on h1

    The expected results are:
    * Test 1: a=5°, ref_a=5°: no possible edge
    * Test 2: a=5°, ref_a=20°: no possible edge
    * Test 3: a=10°, ref_a=10°: one possible edge AB for parent, no possible edge for child
    * Test 4: a=20°, ref_a=5°: two possible edges (AB and AC) for parent, no possible edge for child
    * Test 5: a=20°, ref_a=10°: two possible edges (AB and AC) for parent, one for child (DE)

    """
    # Add graphs
    project_with_data.new_graph(graph_name)
    project_with_data.new_graph(sub_graph_name, references=[graph_name])
    # Add nodes (the possible edge creation is implicitly managed through a view
    project_with_data.add_to_graph_node(graph_name, reference_nodes)
    project_with_data.add_to_graph_node(sub_graph_name, child_nodes)
    with project_with_data.connect() as con:
        cur = con.cursor()
        # First test, with default metadata: no possible edge
        cur.execute(
            "UPDATE albion.metadata SET correlation_angle=5, parent_correlation_angle=5;"
        )
        cur.execute("SELECT start_, end_, graph_id FROM albion.possible_edge;")
        res = cur.fetchall()
        cur.execute("select id, hole_id, from_, to_, parent, graph_id from albion.node")
        assert len(res) == 0
        # Second test, the parent correlation angle is larger, the correlation angle is the same
        # No possible edge, as there is no edge in the parent graph
        cur.execute("UPDATE albion.metadata SET parent_correlation_angle=20;")
        cur.execute("SELECT start_, end_, graph_id FROM albion.possible_edge")
        res = cur.fetchall()
        assert len(res) == 0
        # Third test, the correlation angle and the reference correlation angle are medium:
        # still no possible edge for child graph, as the reference edge has a +10 offset,
        # and the candidate edge has a -10 offset (angle ~ 11.31°)
        cur.execute(
            "UPDATE albion.metadata SET correlation_angle=10, parent_correlation_angle=10;"
        )
        cur.execute("SELECT start_, end_, graph_id FROM albion.possible_edge;")
        res = cur.fetchall()
        assert len(res) == 1
        assert res == [("3", "4", graph_name)]
        # Fourth test, the correlation angle is made big, the reference correlation angle small
        # No possible edge in child graph, even if all the possible edge are valid in parent graph
        cur.execute(
            "UPDATE albion.metadata SET correlation_angle=20, parent_correlation_angle=5;"
        )
        cur.execute("SELECT start_, end_, graph_id FROM albion.possible_edge;")
        res = cur.fetchall()
        assert len(res) == 2
        assert set(res) == {
            ("3", "4", graph_name),
            ("3", "5", graph_name),
        }
        # Fourth test, with a larger reference correlation angle: one possible edge for the child
        # graph
        cur.execute("UPDATE albion.metadata SET parent_correlation_angle=10;")
        cur.execute("SELECT start_, end_, graph_id FROM albion.possible_edge;")
        res = cur.fetchall()
        assert len(res) == 3
        assert set(res) == {
            ("3", "4", graph_name),
            ("3", "5", graph_name),
            ("6", "7", sub_graph_name),
        }


def test_end_node(project_with_data, graph_name, reference_nodes):
    """Test the ending node generation

    One must get 5 ending nodes:

    * 3 towards hole 2 (of coordinates (100, 100, 0)), 1 from node 3 (hole 0) and 2 from nodes 4
      and 5 (hole 1);
    * 2 towards hole 3 (of coordinates (200, 0, 0)), from nodes 4 and 5 (hole 1).

    """
    project_with_data.new_graph(graph_name)
    # Test the creation of nodes in a simple graph
    project_with_data.add_to_graph_node(graph_name, reference_nodes)
    with project_with_data.connect() as con:
        cur = con.cursor()
        cur.execute("UPDATE albion.metadata SET correlation_angle=15.0;")
        con.commit()
    # Accept the possible edge, so as to fill the `_albion.edge` table
    project_with_data.accept_possible_edge(graph_name)
    # Create terminations (one now gets ending nodes within the graph)
    project_with_data.create_terminations(graph_name)
    with project_with_data.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT node_id, hole_id, graph_id FROM albion.end_node;")
        res = cur.fetchall()
    assert len(res) == 5
    assert set(res) == {
        ("4", "3", graph_name),
        ("3", "2", graph_name),
        ("5", "3", graph_name),
        ("4", "2", graph_name),
        ("5", "2", graph_name),
    }
