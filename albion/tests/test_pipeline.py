import tempfile
from pathlib import Path

import pytest
from shapely import wkt

from albion.project import Project

"""
This file simulates the workflow of a new Albion project.

Because of the complexity of interactions with Qt,
integration tests are done only with the `Project` class and without the `Plugin` class.
The plugin actions are (marked 'x' when simulated in these next tests) :
 - [x] New project
 - [x] Import directory
 - [x] Export project
 - [x] Delete project
 - [x] Import project
 - [x] Export holes (dxf & vtk)
 - [ ] Import holes
 - [x] Export layer (dxf & vtk)
 - [ ] Import layer
 - [ ] Compute mineralization
 - [ ] Compute similog resistivity
 - [ ] Update similog markers
 - [x] Create cells
 - [ ] Create subsections
 - [x] New Graph
 - [ ] Delete Graph
 - [x] Previous section
 - [ ] Previous sub section
 - [x] Next section
 - [ ] Next sub section
 - [ ] Create temporary section
 - [x] Refresh selected layers sections
 - [x] Add selection to graph nodes
 - [x] Accept graph possible edges
 - [ ] Create terminations
 - [ ] Create volumes
 - [ ] Export Volume
 - [ ] Export Elementary Volume
 - [ ] Export Sections
 - [ ] (Re)create grid points for raster export
 - [ ] Export rasters from formation
 - [ ] Export rasters from collar
"""


def test_create_project(project, project_srid, project_name):
    # Verifies if all extensions are present
    with project.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT * FROM pg_extension;")
        extension_names = [node[1] for node in cur.fetchall()]
        assert "postgis" in extension_names
        assert "plpython3u" in extension_names
        assert "hstore" in extension_names
        assert "hstore_plpython3u" in extension_names

    # Verifies if all tables are created
    # from _albion schema
    with project.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='_albion'")
        tables = {item[0] for item in cur.fetchall()}
        assert tables == {"resistivity", "graph", "graph_relationship", "metadata", "layer", "formation", "hole",
                          "deviation", "node", "lithology", "edge", "cell", "volume", "group", "group_cell", "facies",
                          "section", "end_node", "named_section", "chemical", "mineralization", "radiometry"}

    # from albion schema
    with project.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='albion'")
        tables = {item[0] for item in cur.fetchall()}
        assert tables == {"resistivity", "resistivity_section", "formation", "formation_section", "lithology",
                          "lithology_section", "facies", "collar", "collar_consensus", "metadata", "layer", "hole",
                          "deviation", "graph", "graph_relationship", "node", "close_collar", "cell", "section",
                          "group", "group_cell", "hole_section", "node_section", "facies_section", "possible_edge",
                          "chemical", "chemical_section", "mineralization", "edge", "edge_section",
                          "possible_edge_section", "volume", "end_node", "half_edge", "normal", "average_normal",
                          "dynamic_end_node", "end_node_section", "section_polygon", "section_intersection",
                          "dynamic_volume", "edge_face", "section_edge", "volume_section", "named_section",
                          "radiometry", "radiometry_section", "mineralization_section", "raw_hole", "raw_deviation"}

    assert project.srid == project_srid
    assert project.name == project_name
    assert not project.has_hole
    assert not project.has_section
    assert not project.has_volume
    assert not project.has_group_cell
    assert not project.has_graph
    assert not project.has_radiometry
    assert not project.has_resistivity_section
    assert not project.has_similog_consensus
    assert not project.has_cell
    assert not project.has_grid


@pytest.mark.dependency()
def test_import_data(project):
    """
    Simulate __import_data from Plugin
    """
    assert not project.has_hole

    # Data can be downloaded from https://gitlab.com/Oslandia/albion_data
    data_directory = Path(__file__).parents[2] / "albion_data" / "nt"
    project.import_data(str(data_directory))

    assert project.has_hole

    # Verifies the number of imported collars
    with project.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT COUNT(1) FROM albion.collar")
        (collar_number,) = cur.fetchone()
        assert collar_number == 178

    # Verifies the number of resistivity parts
    with project.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT COUNT(1) FROM albion.resistivity")
        (collar_number,) = cur.fetchone()
        assert collar_number == 131714

    # Verifies if there is no section (not named_section)
    with project.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT COUNT(1) FROM albion.section")
        (section_number,) = cur.fetchone()
        assert section_number == 0


@pytest.mark.dependency(on=['test_import_data'])
def test_export_project(project_name, project_export_path):
    project = Project(project_name)

    project.export_project(str(project_export_path))

    # Basic tests but the function test_import_project will prove the validity of the export.
    assert project_export_path.exists()
    assert project_export_path.stat().st_size != 0


@pytest.mark.dependency(on=['test_export_project'])
def test_delete_project(project, project_name):
    assert Project.exists(project_name)
    Project.delete(project_name)
    assert not Project.exists(project_name)


@pytest.mark.dependency(on=['test_export_project', 'test_delete_project'])
def test_import_project(project_name, project_export_path):
    assert not Project.exists(project_name)

    project = Project.import_project(project_name, project_export_path)

    # Checks if project is not empty but if data is missing in imported project, some next tests will fail
    with project.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT COUNT(1) FROM albion.resistivity")
        (collar_number,) = cur.fetchone()
        assert collar_number == 131714


@pytest.mark.dependency(on=['test_import_data'])
def test_create_section_anchors(project_name, scale):
    # Uses the project at the previous state
    project = Project(project_name)

    # Create section
    project.create_section_view_0_90(scale)

    # Verifies if sections are created
    with project.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT COUNT(1) FROM albion.section")
        (section_number,) = cur.fetchone()
        assert section_number == 2

    # Verifies if section WE is created
    with project.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT anchor, scale FROM albion.section WHERE id=%s", (f"WE x{scale}",))
        (section_anchor, section_scale) = cur.fetchone()
        # TODO check section_anchor
        assert section_scale == scale

    # Verifies if section WE is created
    with project.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT anchor, scale FROM albion.section WHERE id=%s", (f"SN x{scale}",))
        (section_anchor, section_scale) = cur.fetchone()
        # TODO check section_anchor
        assert section_scale == scale

    # /!\ has_section tests if there are named sections !
    assert not project.has_section


@pytest.mark.dependency(on=['test_import_data'])
def test_create_triangulate_cells(project_name):
    # Uses the project at the previous state
    project = Project(project_name)

    assert not project.has_cell

    project.triangulate()

    assert project.has_cell

    # Counts the number of cells
    # Number found in QGIS assuming the plugin is working as expected.
    with project.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT COUNT(1) FROM albion.cell")
        assert cur.fetchone()[0] == 336


@pytest.mark.dependency(on=['test_create_triangulate_cell'])
def test_create_graph(project_name, graph_name, sub_graph_name):
    # Uses the project at the previous state
    project = Project(project_name)

    assert project.get_existing_graphs() == []

    # Creates the graph
    project.new_graph(graph_name)
    assert project.get_existing_graphs() == [graph_name]

    # Creates a sub-graph
    project.new_graph(sub_graph_name, [graph_name])
    assert set(project.get_existing_graphs()) == {sub_graph_name, graph_name}


@pytest.mark.dependency(on=['test_create_section_anchor', 'test_create_graph'])
def test_create_named_sections(project_name, scale, sn_section_line_1, sn_section_line_2, we_section_line_1):
    # Uses the project at the previous state
    project = Project(project_name)

    assert not project.has_section

    # Gets the linestrings from an Albion project opened in QGIS
    # This process will skip '__add_section_from_selection' logic from Plugin class
    project.add_named_section(f"SN x{scale}", sn_section_line_1)

    assert project.has_section
    with project.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT ST_AsText(geom) FROM albion.named_section")
        section_geom = cur.fetchall()
        assert len(section_geom) == 1
        # Tests if the linestring in db is the same as line_sn_1
        assert wkt.loads(section_geom[0][0]) == sn_section_line_1

    project.add_named_section(f"WE x{scale}", we_section_line_1)

    assert project.has_section
    with project.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT ST_AsText(geom) FROM albion.named_section WHERE section=%s", (f"WE x{scale}",))
        section_geom = cur.fetchall()
        assert len(section_geom) == 1
        # Tests if the linestring in db is the same as line_we_1
        assert wkt.loads(section_geom[0][0]) == we_section_line_1

    # Tests if hole_section and section views reacting correctly to next_section / previous_section
    with project.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT COUNT(1) FROM albion.hole_section")
        (hole_sections,) = cur.fetchone()
        assert hole_sections == 0

    # Only the WE section updated (SN shouldn't be activated)
    project.next_section(f"WE x{scale}")

    with project.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT COUNT(1) FROM albion.hole_section")
        (hole_sections,) = cur.fetchone()
        # Number found in an Albion project opened in QGIS
        # depending on the sections created in 'test_create_named_sections'
        # and assuming the plugin is working as expected.
        assert hole_sections == 18

    # The 2 sections are now updated
    project.next_section(f"SN x{scale}")

    with project.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT COUNT(1) FROM albion.hole_section")
        (hole_sections,) = cur.fetchone()
        # Number found in the same way as above
        assert hole_sections == 30

    # Adds another SN section
    project.add_named_section(f"SN x{scale}", sn_section_line_2)

    # Switches to the 2nd SN section
    project.next_section(f"SN x{scale}")

    with project.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT ST_AsText(geom) FROM albion.section WHERE id=%s", (f"SN x{scale}",))
        (geom_section_sn,) = cur.fetchone()
        assert wkt.loads(geom_section_sn).geoms[0] == sn_section_line_2

    # Tries to switch to a 3rd SN section. Should stay on the 2nd SN section
    project.next_section(f"SN x{scale}")

    with project.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT ST_AsText(geom) FROM albion.section WHERE id=%s", (f"SN x{scale}",))
        (geom_section_sn,) = cur.fetchone()
        assert wkt.loads(geom_section_sn).geoms[0] == sn_section_line_2

    # Switches to the 1st SN section
    project.previous_section(f"SN x{scale}")

    with project.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT ST_AsText(geom) FROM albion.section WHERE id=%s", (f"SN x{scale}",))
        (geom_section_sn,) = cur.fetchone()
        assert wkt.loads(geom_section_sn).geoms[0] == sn_section_line_1

    # Tries to switch to a SN section before the 1st. Should stay on the 1st SN section
    project.previous_section(f"SN x{scale}")

    with project.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT ST_AsText(geom) FROM albion.section WHERE id=%s", (f"SN x{scale}",))
        (geom_section_sn,) = cur.fetchone()
        assert wkt.loads(geom_section_sn).geoms[0] == sn_section_line_1

    # TODO test sub-section


@pytest.mark.dependency(on=['test_create_named_sections'])
def test_refresh_table_sections(project_name, scale):
    # Uses the project at the previous state
    project = Project(project_name)

    with project.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT COUNT(1) FROM albion.formation_section")
        (number_of_formation_sections,) = cur.fetchone()
        assert number_of_formation_sections == 0

    project.refresh_section_geom("formation")

    with project.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT COUNT(1) FROM albion.formation_section")
        (number_of_formation_sections,) = cur.fetchone()
        # Number found in QGIS assuming the plugin is working as expected.
        assert number_of_formation_sections == 210

    # TODO deeper check


@pytest.mark.dependency(on=['test_create_named_sections'])
def test_add_to_graph_nodes_selected_ones(project_name, graph_name, formation_code, expected_edge_sections, expected_node_sections):
    # Uses the project at the previous state
    project = Project(project_name)

    # First graph type check set the type, so it should be true
    assert project.check_graph_type(graph_name, "formation")
    assert not project.check_graph_type(graph_name, "resistivity")
    assert project.check_graph_type(graph_name, "formation")

    with project.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT COUNT(1) FROM albion.possible_edge_section")
        (number_of_possible_edge_section,) = cur.fetchone()
        assert number_of_possible_edge_section == 0

    with project.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT COUNT(1) FROM albion.node_section")
        (number_of_node_section,) = cur.fetchone()
        assert number_of_node_section == 0

    # Fetches form the database all formation sections with the specific code.
    # The goal is to "select" features like user would do in a real project.
    with project.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT from_, to_, hole_id FROM albion.formation_section WHERE code=%s", (formation_code,))
        selection = cur.fetchall()
        selection = [{"from_": feature[0], "to_": feature[1], "hole_id": feature[2]} for feature in selection]

    project.add_to_graph_node(graph_name, selection)

    with project.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT start_, end_, graph_id, section_id, ST_AsText(geom) FROM albion.possible_edge_section")
        possible_edge_sections = set(cur.fetchall())
        assert possible_edge_sections == expected_edge_sections

    with project.connect() as con:
        cur = con.cursor()
        cur.execute(
            "SELECT node_id, hole_id, from_, to_, graph_id, section_id, ST_AsText(geom) FROM albion.node_section"
        )
        node_sections = set(cur.fetchall())
        assert node_sections == expected_node_sections


@pytest.mark.dependency(on=['test_add_to_graph_nodes_selected_ones'])
def test_accept_graph_nodes(project_name, graph_name, formation_code, expected_edge_sections, expected_section_polygons):
    # Uses the project at the previous state
    project = Project(project_name)

    with project.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT COUNT(1) FROM albion.edge_section")
        (number_of_edge_section,) = cur.fetchone()
        assert number_of_edge_section == 0

    with project.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT COUNT(1) FROM albion.section_polygon")
        (number_of_section_polygon,) = cur.fetchone()
        assert number_of_section_polygon == 0

    project.accept_possible_edge(graph_name)

    with project.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT start_, end_, graph_id, section_id, ST_AsText(geom) FROM albion.edge_section")
        edge_sections = set(cur.fetchall())
        assert edge_sections == expected_edge_sections

    with project.connect() as con:
        cur = con.cursor()
        cur.execute("SELECT graph_id, section_id, ST_AsText(geom) FROM albion.section_polygon")
        section_polygons = set(cur.fetchall())

        # Generated from an Albion project opened in QGIS.
        # Depending on the sections created in 'test_create_named_sections' and the fixture 'formation_code'
        # Minor issue : This multipolygon for WE x6 section should have only one part
        # There is a possible edge that missing
        assert section_polygons == expected_section_polygons


@pytest.mark.dependency(on=['test_import_data'])
def test_export_hole_dxf(project_name):
    project = Project(project_name)

    path = Path(tempfile.mkstemp(suffix=".dxf")[1])
    project.export_holes_dxf(str(path))

    # Very simple test (just check if file is not empty)
    assert path.exists()
    assert path.stat().st_size != 0


@pytest.mark.dependency(on=['test_import_data'])
def test_export_hole_vtk(project_name):
    project = Project(project_name)

    path = Path(tempfile.mkstemp(suffix=".vtk")[1])
    project.export_holes_vtk(str(path))

    # Very simple test (just check if file is not empty)
    assert path.exists()
    assert path.stat().st_size != 0


@pytest.mark.dependency(on=['test_import_data'])
def test_export_layer_dxf(project_name):
    project = Project(project_name)

    path = Path(tempfile.mkstemp(suffix=".dxf")[1])
    project.export_layer_dxf("resistivity", str(path))

    # Very simple test (just check if file is not empty)
    assert path.exists()
    assert path.stat().st_size != 0


@pytest.mark.dependency(on=['test_import_data'])
def test_export_layer_vtk(project_name):
    project = Project(project_name)

    path = Path(tempfile.mkstemp(suffix=".vtk")[1])
    project.export_layer_vtk("resistivity", str(path))

    # Very simple test (just check if file is not empty)
    assert path.exists()
    assert path.stat().st_size != 0
