import logging
import os
import shutil
import string
import subprocess
import sys
import time
from pathlib import Path
from typing import List

import pandas as pd
import psycopg2
from dxfwrite import DXFEngine as dxf
from psycopg2 import sql
from psycopg2.extras import LoggingConnection, LoggingCursor
from qgis import processing
from qgis.core import Qgis, QgsDataSourceUri, QgsVectorLayer, QgsWkbTypes
from qgis.utils import iface
from shapely import wkb

try:
    from albion_similog import well_correlation
except ModuleNotFoundError:
    pass

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
# MyLoggingCursor simply sets self.timestamp at start of each query

DIRECTORY_PATH = Path(os.path.dirname(__file__))


class MyLoggingCursor(LoggingCursor):
    def execute(self, query, vars=None):
        self.timestamp = time.time()
        return super(MyLoggingCursor, self).execute(query, vars)

    def callproc(self, procname, vars=None):
        self.timestamp = time.time()
        return super(MyLoggingCursor, self).callproc(procname, vars)


# MyLogging Connection:
#   a) calls MyLoggingCursor rather than the default
#   b) adds resulting execution (+ transport) time via filter()


class MyLoggingConnection(LoggingConnection):
    def filter(self, msg, curs):
        time_ms = int((time.time() - curs.timestamp) * 1000)
        return f"{msg} {time_ms} ms"

    def cursor(self, *args, **kwargs):
        kwargs.setdefault("cursor_factory", MyLoggingCursor)
        return LoggingConnection.cursor(self, *args, **kwargs)


TABLES = [
    {
        "NAME": "radiometry",
        "FIELDS_DEFINITION": "gamma real",
    },
    {
        "NAME": "resistivity",
        "FIELDS_DEFINITION": "rho real",
    },
    {
        "NAME": "formation",
        "FIELDS_DEFINITION": "code integer, comments varchar",
    },
    {
        "NAME": "lithology",
        "FIELDS_DEFINITION": "code integer, comments varchar",
    },
    {
        "NAME": "facies",
        "FIELDS_DEFINITION": "code integer, comments varchar",
    },
    {
        "NAME": "chemical",
        "FIELDS_DEFINITION": """num_sample varchar, element varchar, thickness
        real, gt real, grade real, equi real, comments varchar""",
    },
    {
        "NAME": "mineralization",
        "FIELDS_DEFINITION": "level_ real, oc real, accu real, grade real, comments varchar",
    },
    {
        "NAME": "similog_consensus",
        "FIELDS_DEFINITION": "rho real, global_index int, comments varchar",
    },
    {
        "NAME": "similog_consensus_marker",
        "FIELDS_DEFINITION": "rho real, global_index int, comments varchar",
    },
    {
        "NAME": "similog_aligned_depth",
        "FIELDS_DEFINITION": "comments varchar",
    },
    {
        "NAME": "similog_marker",
        "FIELDS_DEFINITION": "code integer, comments varchar",
    },
]


def find_in_dir(directory, name):
    for filename in os.listdir(directory):
        if filename.find(name) != -1:
            return os.path.abspath(os.path.join(directory, filename))
    return


def get_statements(sql_path: Path) -> List[str]:
    with sql_path.open() as f:
        return f.read().split("\n;\n")[:-1]


class DummyProgress(object):
    def __init__(self):
        if sys.stdout:
            sys.stdout.write("\n")
        self.setPercent(0)

    def __del__(self):
        if sys.stdout:
            sys.stdout.write("\n")

    def setPercent(self, percent):
        l_ = 50
        a = int(round(l_ * float(percent) / 100))
        b = l_ - a
        if sys.stdout:
            sys.stdout.write("\r|" + "#" * a + " " * b + "| % 3d%%" % (percent))
            sys.stdout.flush()


class ProgressBar(object):
    def __init__(self, progress_bar):
        self.__bar = progress_bar
        self.__bar.setMaximum(100)
        self.setPercent(0)

    def setPercent(self, percent):
        self.__bar.setValue(int(percent))


class Project(object):
    def __init__(self, project_name):
        # assert Project.exists(project_name)
        self.__name = project_name
        self.__conn_info = f"service=albion_{project_name}"

    def connect(self):
        con = psycopg2.connect(self.__conn_info)
        return con

    def vacuum(self):
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                conn.set_isolation_level(0)
                cur.execute("vacuum analyze")
                conn.commit()
        finally:
            if conn:
                conn.close()

    @staticmethod
    def exists(project_name):
        with psycopg2.connect("service=albion_maintenance") as con:
            cur = con.cursor()
            con.set_isolation_level(0)
            cur.execute(
                """
                    SELECT pg_terminate_backend(pg_stat_activity.pid)
                    FROM pg_stat_activity
                    WHERE pg_stat_activity.datname=%s
                """,
                (project_name,)
            )

            cur.execute(
                """
                    SELECT count(1)
                    FROM pg_catalog.pg_database
                    WHERE datname=%s
                """,
                (project_name,)
            )
            res = cur.fetchone()[0] == 1
            return res

    @staticmethod
    def delete(project_name):
        assert Project.exists(project_name)
        conn = psycopg2.connect("service=albion_maintenance")
        try:
            with conn.cursor() as cur:
                conn.set_isolation_level(0)
                cur.execute(
                    """
                        SELECT pg_terminate_backend(pg_stat_activity.pid)
                        FROM pg_stat_activity
                        WHERE pg_stat_activity.datname=%s
                    """,
                    (project_name,)
                )
                cur.execute(
                    sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(project_name))
                )

                cur.execute(
                    """
                        SELECT count(1)
                        FROM pg_catalog.pg_database
                        WHERE datname=%s
                    """,
                    (project_name,)
                )
                conn.commit()
        finally:
            if conn:
                conn.close()

    @staticmethod
    def create_db(project_name):
        assert not Project.exists(project_name)

        conn = psycopg2.connect("service=albion_maintenance")
        try:
            with conn.cursor() as cur:
                conn.set_isolation_level(0)
                cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(project_name)))
                conn.commit()
        finally:
            if conn:
                conn.close()

    @staticmethod
    def create(project_name, srid):
        Project.create_db(project_name)

        project = Project(project_name)
        with project.connect() as con:
            cur = con.cursor()
            cur.execute("CREATE EXTENSION IF NOT EXISTS postgis")
            cur.execute("CREATE EXTENSION IF NOT EXISTS plpython3u")
            cur.execute("CREATE EXTENSION IF NOT EXISTS hstore")
            cur.execute("CREATE EXTENSION IF NOT EXISTS hstore_plpython3u")

            pathname = DIRECTORY_PATH / "elementary_volume" / "__init__.py"
            with pathname.open() as f:
                include_elementary_volume = f.read()

            for sql_file in ("_albion.sql", "albion_collar.sql", "albion.sql"):
                sql_path = DIRECTORY_PATH / "albion_db" / sql_file
                for statement in get_statements(sql_path):
                    cur.execute(
                        statement.replace("$SRID", str(srid)).replace(
                            "$INCLUDE_ELEMENTARY_VOLUME", include_elementary_volume
                        )
                    )
            con.commit()

        for table in [t for t in TABLES if "similog" not in t["NAME"]]:
            table["SRID"] = srid
            project.add_table(table)

        return project

    def add_table(self, table, values=None, view_only=False):
        """
        table: a dict with keys
            NAME: the name of the table to create
            FIELDS_DEFINITION: the sql definition (name type) of the
                "additional" fields (i.e. excludes hole_id, from_ and to_)
            SRID: the project's SRID
        values: list of tuples (hole_id, from_, to_, ...)
        """

        fields = [f.split()[0].strip() for f in table["FIELDS_DEFINITION"].split(",")]
        table["FIELDS"] = ", ".join(fields)
        table["T_FIELDS"] = ", ".join(
            ["t.{}".format(f.replace(" ", "")) for f in fields]
        )
        table["FORMAT"] = ",".join([" %s" for v in fields])
        table["NEW_FIELDS"] = ",".join([f"new.{v}" for v in fields])
        table["SET_FIELDS"] = ",".join([f"{v}=new.{v}" for v in fields])
        with self.connect() as con:
            cur = con.cursor()

            sql_files = (
                ("albion_table.sql",)
                if view_only
                else ("_albion_table.sql", "albion_table.sql")
            )

            for sql_file in sql_files:
                sql_path = DIRECTORY_PATH / "albion_db" / sql_file
                for statement in get_statements(sql_path):
                    cur.execute(string.Template(statement).substitute(table))

            if values is not None:
                cur.executemany(
                    """
                        INSERT INTO albion.{NAME}(hole_id, from_, to_, {FIELDS})
                        VALUES (%s, %s, %s, {FORMAT})
                    """.format(**table),
                    values,
                )
                cur.execute(
                    """
                        REFRESH MATERIALIZED VIEW albion.{NAME}_section_geom_cache
                    """.format(**table)
                )
            con.commit()
        self.vacuum()

    def update(self):
        """reload schema albion without changing data"""

        with self.connect() as con:
            cur = con.cursor()

            cur.execute("SELECT srid FROM albion.metadata")
            (srid,) = cur.fetchone()
            # test if version number is in metadata
            cur.execute(
                """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = 'metadata' AND column_name='version'
                """
            )
            if cur.fetchone():
                # here goes future upgrades
                cur.execute("SELECT version FROM _albion.metadata")
                ver = cur.fetchone()[0]
                if ver == "2.0":
                    if self.__has_cell():
                        sql_path = DIRECTORY_PATH / "albion_db" / "albion_raster.sql"
                        for statement in get_statements(sql_path):
                            cur.execute(statement)

                    sql_path = DIRECTORY_PATH / "updates" / "_update_edge_detection.sql"
                    for statement in get_statements(sql_path):
                        cur.execute(statement)

                if ver == "2.3":
                    sql_path = DIRECTORY_PATH / "updates" / "elementary_volumes_2.3-2.4.sql"
                    with sql_path.open() as f:
                        cur.execute(f.read())

                    sql_path = DIRECTORY_PATH / "updates" / "_albion_v2.3-v2.4.sql"
                    for statement in get_statements(sql_path):
                        cur.execute(statement.replace("$SRID", str(srid)))

                if ver == "2.4":
                    sql_path = DIRECTORY_PATH / "updates" / "elementary_volumes_2.4-2.5.sql"
                    with sql_path.open() as f:
                        cur.execute(f.read().replace("$SRID", str(srid)))

                    sql_path = DIRECTORY_PATH / "updates" / "_albion_v2.4-v2.5.sql"
                    for statement in get_statements(sql_path):
                        cur.execute(statement.replace("$SRID", str(srid)))
                        
                    sql_path = DIRECTORY_PATH / "updates" / "_formation_section_edit.sql"
                    for statement in get_statements(sql_path):
                        cur.execute(statement.replace("$SRID", str(srid)))

                cur.execute("UPDATE _albion.metadata SET version = '2.5'")
                con.commit()

            else:
                cur.execute("drop schema if exists albion cascade")
                # old albion version, we upgrade the data
                sql_path = DIRECTORY_PATH / "updates" / "_albion_v1_to_v2.sql"
                for statement in get_statements(sql_path):
                    cur.execute(statement.replace("$SRID", str(srid)))

                pathname = DIRECTORY_PATH / "elementary_volume" / "__init__.py"
                with pathname.open() as f:
                    include_elementary_volume = f.read()

                for sql_file in ("albion_collar.sql", "albion.sql"):
                    sql_path = DIRECTORY_PATH / "albion_db" / sql_file
                    for statement in get_statements(sql_path):
                        cur.execute(
                            statement.replace("$SRID", str(srid)).replace(
                                "$INCLUDE_ELEMENTARY_VOLUME", include_elementary_volume
                            )
                        )

                con.commit()

                cur.execute("SELECT name, fields_definition FROM albion.layer")
                tables = [
                    {"NAME": r[0], "FIELDS_DEFINITION": r[1]} for r in cur.fetchall()
                ]

                for table in tables:
                    table["SRID"] = str(srid)
                    self.add_table(table, view_only=True)

                self.vacuum()

    def export_sections_obj(self, graph_id, filename):
        with self.connect() as con:
            cur = con.cursor()
            # TODO: Too long, must be a SQL function or in a sql file
            cur.execute(
                f"""
                    WITH hole_idx AS (
                        SELECT s.id AS section_id, h.id AS hole_id
                        FROM _albion.named_section AS s
                        JOIN _albion.hole AS h ON s.geom && h.geom AND
                        st_intersects(s.geom, st_startpoint(h.geom))
                    )
                    SELECT
                        albion.to_obj(st_collectionhomogenize(st_collect(ef.geom)))
                    FROM albion.all_edge AS e
                    JOIN hole_idx AS hs ON hs.hole_id = e.start_
                    JOIN hole_idx AS he ON he.hole_id = e.end_ AND
                        he.section_id = hs.section_id
                    JOIN albion.edge_face AS ef ON ef.start_ = e.start_ AND
                        ef.end_ = e.end_ AND NOT st_isempty(ef.geom)
                    WHERE ef.graph_id='{graph_id}'
                """
            )
            with open(filename, "w") as f:
                f.write(cur.fetchone()[0])

    def export_sections_dxf(self, graph_id, filename):
        with self.connect() as con:
            cur = con.cursor()
            # TODO: Too long, must be a SQL function or in a sql file
            cur.execute(
                f"""
                    WITH hole_idx AS (
                        SELECT s.id AS section_id, h.id AS hole_id
                        FROM _albion.named_section AS s
                        JOIN _albion.hole AS h ON s.geom && h.geom AND
                            st_intersects(s.geom, st_startpoint(h.geom))
                    )
                    SELECT st_collectionhomogenize(st_collect(ef.geom))
                    FROM albion.all_edge AS e
                    JOIN hole_idx AS hs ON hs.hole_id = e.start_
                    JOIN hole_idx AS he ON he.hole_id = e.end_ AND
                        he.section_id = hs.section_id
                    JOIN albion.edge_face AS ef ON ef.start_ = e.start_ AND
                        ef.end_ = e.end_ AND NOT st_isempty(ef.geom)
                    WHERE ef.graph_id='{graph_id}'
                """
            )

            drawing = dxf.drawing(filename)
            m = wkb.loads(bytes.fromhex(cur.fetchone()[0]))
            for p in m:
                r = p.exterior.coords
                drawing.add(
                    dxf.face3d([tuple(r[0]), tuple(r[1]), tuple(r[2])], flags=1)
                )
            drawing.save()

    def __srid(self):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute("SELECT srid FROM albion.metadata")
            (srid,) = cur.fetchone()
        return srid

    def __getattr__(self, name):
        if name == "has_hole":
            return self.__has_hole()
        elif name == "has_section":
            return self.__has_section()
        elif name == "has_volume":
            return self.__has_volume()
        elif name == "has_group_cell":
            return self.__has_group_cell()
        elif name == "has_graph":
            return self.__has_graph()
        elif name == "has_radiometry":
            return self.__has_radiometry()
        elif name == "has_resistivity_section":
            return self.__has_resistivity_section()
        elif name == "has_similog_consensus":
            return self.__has_similog_consensus()
        elif name == "has_cell":
            return self.__has_cell()
        elif name == "has_grid":
            return self.__has_grid()
        elif name == "name":
            return self.__name
        elif name == "srid":
            return self.__srid()

        return super().__getattribute__(name)

    def __has_cell(self):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute("SELECT COUNT(1) FROM albion.cell")
            return cur.fetchone()[0] > 0

    def __has_grid(self):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(
                """
                    SELECT EXISTS(SELECT * FROM information_schema.tables
                    WHERE table_schema='_albion' AND table_name='grid')
                """
            )
            return cur.fetchone()[0]

    def __has_hole(self):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(
                """
                    SELECT COUNT(1) FROM albion.hole
                    WHERE geom IS NOT NULL
                """
            )
            return cur.fetchone()[0] > 0

    def __has_volume(self):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute("SELECT COUNT(1) FROM albion.volume")
            return cur.fetchone()[0] > 0

    def __has_section(self):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute("SELECT COUNT(1) FROM albion.named_section")
            return cur.fetchone()[0] > 0

    def __has_group_cell(self):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute("SELECT COUNT(1) FROM albion.group_cell")
            return cur.fetchone()[0] > 0

    def __has_graph(self):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute("SELECT COUNT(1) FROM albion.graph")
            return cur.fetchone()[0] > 0

    def __has_radiometry(self):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute("SELECT COUNT(1) FROM albion.radiometry")
            return cur.fetchone()[0] > 0

    def __has_resistivity_section(self):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute("SELECT COUNT(1) FROM albion.resistivity_section")
            return cur.fetchone()[0] > 0

    def __has_similog_consensus(self):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(
                """
                    SELECT COUNT(1) FROM information_schema.tables
                    WHERE table_schema='albion' AND table_name='similog_consensus'
                """
            )
            return cur.fetchone()[0] > 0

    def __copy_data(self, dir_, filename, table, cols):
        data_filepath = find_in_dir(dir_, filename)
        if data_filepath is None:
            return
        with self.connect() as con:
            cur = con.cursor()

            with open(data_filepath, "r") as fobj:
                column_names = sql.SQL(', ').join([sql.Identifier(col) for col in cols])
                query = sql.SQL("""COPY {table} ( {column_names} )
                        FROM STDIN WITH CSV HEADER DELIMITER ';'""").format(
                    table=sql.SQL(table),
                    column_names=column_names)
                cur.copy_expert(query, fobj)

    def import_data(self, dir_, progress=DummyProgress()):
        with self.connect() as con:
            cur = con.cursor()

            self.__copy_data(
                dir_,
                "collar",
                "albion.raw_hole",
                ("id", "x", "y", "z", "depth_", "date_", "comments"),
            )

            cur.execute("SELECT albion.update_consensus_hole();")
            progress.setPercent(5)

            self.__copy_data(
                dir_,
                "deviation",
                "_albion.deviation",
                ("hole_id", "from_", "dip", "azimuth"),
            )

            progress.setPercent(10)

            cur.execute(
                """
                    UPDATE _albion.hole
                    SET geom = albion.hole_geom(id)
                """
            )

            progress.setPercent(15)

            self.__copy_data(
                dir_, "avp", "_albion.radiometry", ("hole_id", "from_", "to_", "gamma")
            )

            progress.setPercent(20)

            self.__copy_data(
                dir_,
                "formation",
                "_albion.formation",
                ("hole_id", "from_", "to_", "code", "comments"),
            )

            progress.setPercent(25)

            self.__copy_data(
                dir_,
                "lithology",
                "_albion.lithology",
                ("hole_id", "from_", "to_", "code", "comments"),
            )

            progress.setPercent(30)

            self.__copy_data(
                dir_,
                "facies",
                "_albion.facies",
                ("hole_id", "from_", "to_", "code", "comments"),
            )

            progress.setPercent(35)

            self.__copy_data(
                dir_, "resi", "_albion.resistivity", ("hole_id", "from_", "to_", "rho")
            )

            progress.setPercent(40)

            self.__copy_data(
                dir_,
                "chemical",
                "_albion.chemical",
                (
                    "hole_id",
                    "from_",
                    "to_",
                    "num_sample",
                    "element",
                    "thickness",
                    "gt",
                    "grade",
                    "equi",
                    "comments",
                ),
            )

            progress.setPercent(45)
            progress.setPercent(100)

            con.commit()

        self.vacuum()

    def triangulate(self):
        """Do a constrained triangulation on the Albion collar set.

        If the triangulation does already exist, one refreshes the hole_nodes and cells
        materialized views.

        """
        with self.connect() as con:
            cur = con.cursor()
            cur.execute("SELECT albion.triangulate()")
            if not self.__has_cell():
                # Initialize the triangulation
                sql_path = DIRECTORY_PATH / "albion_db" /"albion_raster.sql"
                for statement in get_statements(sql_path):
                    cur.execute(statement)
            else:
                # Here there are already some cells, the triangulation has still be done. One
                # refreshs the related materialized views.
                cur.execute("REFRESH MATERIALIZED VIEW _albion.hole_nodes")
                cur.execute("REFRESH MATERIALIZED VIEW _albion.cells")
            con.commit()

    def create_sections(self):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute("REFRESH MATERIALIZED VIEW albion.section_geom")
            con.commit()

    def execute_script(self, sql_file):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute("SELECT srid FROM albion.metadata")
            (srid,) = cur.fetchone()

            for statement in get_statements(Path(sql_file)):
                cur.execute(statement.replace("$SRID", str(srid)))
            con.commit()

    def get_existing_graphs(self):
        """Provides the list of existing graphs within the project."""
        with self.connect() as con:
            cur = con.cursor()
            cur.execute("SELECT id FROM albion.graph;")
            res = cur.fetchall()
            return [r[0] for r in res]

    def new_graph(self, graph_id, references=None):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute( # TODO add user alert, if the graph already exists, it is deleted with no warning !
                """
                    DELETE FROM albion.graph CASCADE
                    WHERE id=%s;
                """,
                (graph_id,)
            )
            cur.execute("INSERT INTO albion.graph(id) VALUES (%s);", (graph_id,))
            if references is not None:
                for parent in references:
                    cur.execute(
                        """
                            INSERT INTO albion.graph_relationship(parent_id, child_id)
                            VALUES (%s, %s);
                        """,
                        (parent, graph_id)
                    )
            con.commit()

    def delete_graph(self, graph_id):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(
                """
                    DELETE FROM albion.graph CASCADE
                    WHERE id=%s;
                """,
                (graph_id,)
            )

    def previous_section(self, section):
        if not section:
            return

        with self.connect() as con:
            cur = con.cursor()
            cur.execute(
                """
                    UPDATE albion.section
                    SET geom=COALESCE(albion.previous_section(%s), geom)
                    WHERE id=%s
                """,
                (section, section),
            )
            con.commit()

    def next_section(self, section):
        if not section:
            return

        with self.connect() as con:
            cur = con.cursor()
            cur.execute(
                """
                    UPDATE albion.section
                    SET geom=COALESCE(albion.next_section(%s), geom)
                    WHERE id=%s
                """,
                (section, section),
            )
            con.commit()

    def next_subsection(self, section):
        with self.connect() as con:
            # select section from distance
            cur = con.cursor()
            cur.execute(
                """
                    SELECT sg.group_id
                    FROM albion.section_geom sg
                    JOIN albion.section s ON s.id=sg.section_id
                    WHERE s.id=%s
                    ORDER BY ST_Distance(s.geom, sg.geom),
                        ST_HausdorffDistance(s.geom, sg.geom) ASC
                    LIMIT 1
                """,
                (section,)
            )
            res = cur.fetchone()
            if not res:
                return

            group = res[0] or 0
            # select geom for next
            cur.execute(
                """
                    SELECT geom FROM albion.section_geom
                    WHERE section_id=%s
                    AND group_id > %s
                    ORDER BY group_id ASC
                    LIMIT 1
                """,
                (section, group)
            )
            res = cur.fetchone()
            # update section
            if not res:
                return

            cur.execute(
                """
                    UPDATE albion.section
                    SET geom=st_multi(%s::geometry)
                    WHERE id=%s
                """,
                (res[0], section)
            )
            con.commit()

    def previous_subsection(self, section):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(
                """
                    SELECT sg.group_id
                    FROM albion.section_geom sg
                    JOIN albion.section s ON s.id=sg.section_id
                    WHERE s.id=%s
                    ORDER BY
                        ST_Distance(s.geom, sg.geom),
                        ST_HausdorffDistance(s.geom, sg.geom) ASC
                    LIMIT 1
                """,
                (section,)
            )
            res = cur.fetchone()
            if not res:
                return

            group = res[0] or 0
            cur.execute(
                """
                SELECT geom
                FROM albion.section_geom
                WHERE section_id=%s AND group_id < %s
                ORDER BY group_id DESC
                LIMIT 1
                """,
                (section, group)
            )
            res = cur.fetchone()
            if not res:
                return

            cur.execute(
                """
                    UPDATE albion.section
                    SET geom=st_multi(%s::geometry)
                    WHERE id=%s
                """,
                (res[0], section)
            )
            con.commit()

    def create_group(self, section, ids):
        with self.connect() as con:
            # add group
            cur = con.cursor()
            cur.execute(
                """
                    INSERT INTO albion.group(id)
                    VALUES ((SELECT COALESCE(MAX(id)+1, 1) FROM albion.group))
                    RETURNING id
                """
            )
            (group,) = cur.fetchone()
            cur.executemany(
                """
                    INSERT INTO albion.group_cell(section_id, cell_id, group_id)
                    VALUES(%s, %s, %s)
                """,
                ((section, id_, group) for id_ in ids),
            )
            con.commit()

    def sections(self):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute("SELECT id FROM albion.section")
            return [id_ for id_, in cur.fetchall()]

    def graphs(self):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute("SELECT id FROM albion.graph")
            return [id_ for id_, in cur.fetchall()]

    def compute_mineralization(self, cutoff, ci, oc):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(
                "DELETE FROM albion.mineralization WHERE level_=%s",
                (cutoff,)
            )
            cur.execute(
                f"""
                    INSERT INTO
                        albion.mineralization(hole_id,
                                              level_, from_, to_, oc, accu, grade)
                    SELECT hole_id, (t.r).level_, (t.r).from_, (t.r).to_,
                        (t.r).oc, (t.r).accu, (t.r).grade
                    FROM (
                    SELECT hole_id, albion.segmentation(
                        array_agg(gamma ORDER BY from_),
                        array_agg(from_ ORDER BY from_),
                        array_agg(to_ ORDER BY from_),
                        {ci}, {oc}, {cutoff}) AS r
                    FROM albion.radiometry
                    GROUP BY hole_id
                    ) AS t
                """
            )
            cur.execute(
                """
                    REFRESH MATERIALIZED VIEW
                    albion.mineralization_section_geom_cache
                """
            )
            con.commit()

    def add_similog_tables(self):
        """Add the similog table to the Albion database; they are required for running Similog-related
        features.

        """
        with self.connect() as con:
            cur = con.cursor()
            cur.execute("SELECT srid FROM albion.metadata")
            (srid,) = cur.fetchone()
            cur.execute("DROP TABLE IF EXISTS _albion.similog_consensus CASCADE;")
            cur.execute(
                "DROP TABLE IF EXISTS _albion.similog_consensus_marker CASCADE;"
            )
            cur.execute("DROP TABLE IF EXISTS _albion.similog_aligned_depth CASCADE;")
            cur.execute("DROP TABLE IF EXISTS _albion.similog_marker CASCADE;")
            cur.execute("DELETE FROM _albion.layer WHERE name LIKE 'similog%';")
        logger.info("Add Similog tables into the database...")
        for table in [t for t in TABLES if "similog" in t["NAME"]]:
            table["SRID"] = srid
            self.add_table(table)

    def insert_similog_consensus(self, consensus, table_name):
        """Insert the consensus similog resistivity at the ghost collar. The data is inserted into
        ``table_name`` table.

        Parameters
        ----------
        consensus : pd.DataFrame
            Log consensus for resistitivity dataseries
        table_name : str
            Name of the table where to store consensus data
        """
        if not self.__has_similog_consensus():
            return

        with self.connect() as con:
            cur = con.cursor()
            logger.info("Write Similog consensus into table %s...", table_name)
            cur.execute(
                sql.SQL("DELETE FROM albion.{} WHERE True;").format(sql.Identifier(table_name))
            )

            insert_query = sql.SQL(
                f"""
                    INSERT INTO albion.{table_name}(global_index, rho, from_, hole_id)
                    VALUES {",".join(["%s"] * consensus.shape[0])}
                """
            )

            inserted_values = [
                c + ("CONSENSUS_",)
                for c in consensus.drop(columns=["freq"]).itertuples(
                    index=False, name=None
                )
            ]
            cur.execute(insert_query, inserted_values)

    def insert_similog_depths(self, depths):
        """Insert the consensus similog depths, that models aligned collars for resistivity consensus
        modelling.

        Parameters
        ----------
        consensus : pd.DataFrame
            Log consensus for resistitivity dataseries

        """
        if not self.__has_similog_consensus():
            return

        with self.connect() as con:
            cur = con.cursor()
            logger.info(
                "Write Similog consensus into table albion.similog_aligned_depth..."
            )
            cur.execute("DELETE FROM albion.similog_aligned_depth WHERE TRUE;")
            insert_query = sql.SQL(
                f"""
                    INSERT INTO albion.similog_aligned_depth(hole_id, from_)
                    VALUES {",".join(["%s"] * depths.shape[0])}
                """
            )
            inserted_values = [c for c in depths.itertuples(index=False, name=None)]
            cur.execute(insert_query, inserted_values)

    def insert_similog_markers(self, markers):
        """Fill in the resistivity marker table with similog results

        Parameters
        ----------
        markers : pd.DataFrame
            Similog marker, with "hole_id", "from", "to" and "code"
        """
        if not self.__has_similog_consensus():
            return

        assert all(col in markers.columns for col in ("hole_id", "from", "to", "code"))
        with self.connect() as con:
            cur = con.cursor()
            logger.info("Write Similog markers into database...")
            tops = markers[["hole_id", "from", "to", "code"]]
            cur.execute(
                f"""
                    DELETE FROM albion.similog_marker
                    WHERE hole_id IN {tuple(tops.hole_id.unique())}
                """
            )
            insert_query = sql.SQL(
                f"""
                    INSERT INTO albion.similog_marker(hole_id, from_, to_, code)
                    VALUES {",".join(["%s"] * tops.shape[0])}
                """
            )
            inserted_values = list(tops.itertuples(index=False, name=None))
            cur.execute(insert_query, inserted_values)
            # Refresh the materialized view associated to similog marker sections
            logger.info("Refresh marker section materialized view...")
            cur.execute(
                "REFRESH MATERIALIZED VIEW albion.similog_marker_section_geom_cache"
            )

    def update_similog_markers(self):
        """Update similog markers and refresh the associated marker section materialized view."""
        if not self.__has_similog_consensus():
            return

        with self.connect() as con:
            # Read the user-defined depth from the consensus marker table
            logger.info("Read the similog consensus markers...")
            tops = pd.read_sql(
                "SELECT from_ FROM albion.similog_consensus_marker;",
                con
            )
            tops = tops["from_"]
            # Read the aligned depths from the global depth table
            aligned_depths = pd.read_sql(
                "SELECT hole_id, from_ AS FROM FROM albion.similog_aligned_depth;",
                con
            )
            # Read the consensus from the consensus table
            consensus = pd.read_sql(
                "SELECT global_index, hole_id, from_ FROM albion.similog_consensus;",
                con,
            )
        markers = well_correlation.compute_markers(
            tops, aligned_depths, consensus, "from_"
        )
        self.insert_similog_markers(markers)

    def export_volume_obj(self, graph_id, filename):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(
                """
                    SELECT albion.to_obj(
                        albion.volume_union(
                            st_collectionhomogenize(st_collect(triangulation))))
                    FROM albion.volume
                    WHERE graph_id=%s
                    AND albion.is_closed_volume(triangulation)
                    AND  albion.volume_of_geom(triangulation) > 1
                """,
                (graph_id,)
            )

            with open(filename, "w") as f:
                f.write(cur.fetchone()[0])

    def export_volume_dxf(self, graph_id, filename):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(
                """
                    SELECT
                    albion.volume_union(
                        st_collectionhomogenize(st_collect(triangulation)))
                    FROM albion.volume
                    WHERE graph_id=%s
                    AND albion.is_closed_volume(triangulation)
                    AND  albion.volume_of_geom(triangulation) > 1
                """,
                (graph_id,)
            )
            drawing = dxf.drawing(filename)
            m = wkb.loads(bytes.fromhex(cur.fetchone()[0]))
            for p in m:
                r = p.exterior.coords
                drawing.add(
                    dxf.face3d([tuple(r[0]), tuple(r[1]), tuple(r[2])], flags=1)
                )
            drawing.save()

    def export_volume_errors_obj(self, graph_id, filename):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(
                """
                    SELECT
                    albion.to_obj(
                        st_collectionhomogenize(st_collect(triangulation)))
                    FROM albion.volume
                    WHERE graph_id=%s
                    AND (NOT albion.is_closed_volume(triangulation) OR
                        albion.volume_of_geom(triangulation) <= 1)
                """,
                (graph_id,)
            )
            with open(filename, "w") as f:
                f.write(cur.fetchone()[0])

    def export_elementary_volume_obj(
        self, graph_id, cell_ids, outdir, closed_only=False
    ):
        with self.connect() as con:
            cur = con.cursor()
            formatted_cell_ids = ",".join([f"'{c}'" for c in cell_ids])
            cur.execute(
                f"""
                    SELECT cell_id,
                    row_number() over(partition by cell_id ORDER BY closed DESC),
                    obj, closed
                    FROM (
                        SELECT cell_id, albion.to_obj(triangulation) AS obj,
                            albion.is_closed_volume(triangulation) AS closed
                        FROM albion.volume
                        WHERE cell_id IN ({formatted_cell_ids}) AND graph_id='{graph_id}'
                        ) AS t
                """
            )
            for cell_id, i, obj, closed in cur.fetchall():
                if closed_only and not closed:
                    continue

                filename = "{}_{}_{}_{}.obj".format(
                    cell_id, graph_id, "closed" if closed else "opened", i
                )
                pathname = Path(outdir) / filename
                with pathname.open('w') as f:
                    f.write(obj[0])

    def export_elementary_volume_dxf(
        self, graph_id, cell_ids, outdir, closed_only=False
    ):
        with self.connect() as con:
            cur = con.cursor()
            formatted_cell_ids = ",".join([f"'{c}'" for c in cell_ids])
            cur.execute(
                f"""
                    SELECT cell_id,
                    row_number() over(partition by cell_id ORDER BY closed DESC),
                    geom, closed
                    FROM (
                        SELECT cell_id, triangulation AS geom,
                            albion.is_closed_volume(triangulation) AS closed
                        FROM albion.volume
                        WHERE cell_id IN ({formatted_cell_ids}) AND graph_id='{graph_id}'
                        ) AS t
                """
            )

            for cell_id, i, wkb_geom, closed in cur.fetchall():
                geom = wkb.loads(bytes.fromhex(wkb_geom))
                if closed_only and not closed:
                    continue

                filename = "{}_{}_{}_{}.dxf".format(
                    cell_id, graph_id, "closed" if closed else "opened", i
                )
                path = os.path.join(outdir, filename)
                drawing = dxf.drawing(path)

                for p in geom:
                    r = p.exterior.coords
                    drawing.add(
                        dxf.face3d([tuple(r[0]), tuple(r[1]), tuple(r[2])], flags=1)
                    )
                drawing.save()

    def export_holes_vtk(self, filename):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(
                """
                    SELECT albion.to_vtk(st_collect(geom))
                    FROM albion.hole
                """
            )
            with open(filename, "w") as f:
                f.write(cur.fetchone()[0])

    def export_holes_dxf(self, filename):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(
                """
                    SELECT st_collect(geom)
                    FROM albion.hole
                """
            )
            drawing = dxf.drawing(filename)
            m = wkb.loads(bytes.fromhex(cur.fetchone()[0]))
            for l_ in m.geoms:
                drawing.add(dxf.polyline(list(l_.coords)))
            drawing.save()

    def export_layer_vtk(self, table, filename):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(
                sql.SQL("""
                    SELECT albion.to_vtk(
                        st_collect(albion.hole_piece(from_, to_, hole_id)))
                    FROM albion.{}
                """).format(sql.Identifier(table))
            )
            with open(filename, "w") as f:
                f.write(cur.fetchone()[0])

    def export_layer_dxf(self, table, filename):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(
                sql.SQL("""
                    SELECT st_collect(albion.hole_piece(from_, to_, hole_id))
                    FROM albion.{}
                """).format(sql.Identifier(table))
            )
            drawing = dxf.drawing(filename)
            m = wkb.loads(bytes.fromhex(cur.fetchone()[0]))
            for l_ in m.geoms:
                drawing.add(dxf.polyline(list(l_.coords)))
            drawing.save()

    def create_volumes(self, graph_id):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(
                """
                    DELETE FROM albion.volume
                    WHERE graph_id=%s
                """,
                (graph_id,)
            )
            cur.execute(
                f"""
                    INSERT INTO
                        _albion.volume(graph_id, cell_id, triangulation,
                            face1, face2, face3)
                    SELECT graph_id, cell_id, geom, face1, face2, face3
                    FROM albion.dynamic_volume
                    WHERE graph_id='{graph_id}'
                    AND geom IS NOT NULL
                """
            )
            con.commit()

    def create_terminations(self, graph_id):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(
                """
                    INSERT INTO albion.end_node(geom, node_id, hole_id, graph_id)
                    SELECT geom, node_id, hole_id, graph_id
                    FROM albion.dynamic_end_node
                    WHERE graph_id=%s
                    ON CONFLICT (node_id, hole_id, graph_id) DO NOTHING
                """,
                (graph_id,)
            )
            con.commit()

    def export_project(self, filename):
        pg_dump_path = shutil.which("pg_dump")
        if pg_dump_path is None:
            iface.messageBar().pushMessage(
                "Export error",
                """Can not export the database.
                                           pg_dump is missing""",
                level=Qgis.Critical,
            )
            return

        subprocess.Popen(
            [
                pg_dump_path,
                f"service=albion_{self.name}",
                "-O",
                "-x",
                "-f",
                filename,
            ]
        ).communicate()

    @staticmethod
    def import_project(project_name, filename):
        Project.create_db(project_name)
        psql_path = shutil.which("psql")
        if psql_path is None:
            iface.messageBar().pushMessage(
                "Import error",
                """Can not import the database.
                                           psql is missing""",
                level=Qgis.critical,
            )
            return

        subprocess.Popen(
            [psql_path, f"service=albion_{project_name}", "-f", filename]
        ).communicate()

        project = Project(project_name)
        project.update()
        project.create_sections()
        return project

    def create_section_view_0_90(self, z_scale):
        """create default WE and SN section views with magnifications

        we position anchors south and east in order to have the top of
        the section with a 50m margin from the extent of the holes
        """

        with self.connect() as con:
            cur = con.cursor()
            cur.execute(
                """
                    SELECT st_3dextent(geom)
                    FROM albion.hole
                """
            )
            ext = cur.fetchone()[0] \
                .replace("BOX3D(", "") \
                .replace(")", "") \
                .split(",")
            ext = [
                [float(c) for c in ext[0].split()],
                [float(c) for c in ext[1].split()],
            ]

            cur.execute("SELECT srid FROM albion.metadata")
            (srid,) = cur.fetchone()
            cur.execute(
                """
                    INSERT INTO albion.section(id, anchor, scale)
                    VALUES(
                        'SN x{z_scale}',
                        'SRID={srid};LINESTRING({x} {ybottom}, {x} {ytop})'::geometry,
                        {z_scale}
                    )
                """.format(
                    z_scale=z_scale,
                    srid=srid,
                    x=ext[1][0] + 50 + z_scale * ext[1][2],
                    ybottom=ext[0][1],
                    ytop=ext[1][1],
                )
            )

            cur.execute(
                """
                    INSERT INTO albion.section(id, anchor, scale)
                    VALUES(
                        'WE x{z_scale}',
                        'SRID={srid};LINESTRING({xleft} {y}, {xright} {y})'::geometry,
                        {z_scale}
                    )
                """.format(
                    z_scale=z_scale,
                    srid=srid,
                    y=ext[0][1] - 50 - z_scale * ext[1][2],
                    xleft=ext[0][0],
                    xright=ext[1][0],
                )
            )

            con.commit()

    def refresh_section_geom(self, table):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(
                """
                    SELECT COUNT(1)
                    FROM albion.layer
                    WHERE name=%s
                """,
                (table,)
            )
            if cur.fetchone()[0]:
                cur.execute(
                    sql.SQL("""
                        REFRESH MATERIALIZED VIEW
                        albion.{}
                    """).format(sql.Identifier(table + "_section_geom_cache"))
                )
                con.commit()

    def closest_hole_id(self, x, y):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute("SELECT srid FROM albion.metadata")
            (srid,) = cur.fetchone()
            cur.execute(
                f"""
                    SELECT id
                    FROM albion.hole
                    WHERE st_dwithin(geom,
                        'SRID={srid} ;POINT({x} {y})'::geometry, 25)
                    ORDER BY st_distance('SRID={srid};POINT({x} {y})'::geometry,
                        geom)
                    LIMIT 1
                """
            )
            res = cur.fetchone()
            if not res:
                cur.execute(
                    f"""
                        SELECT hole_id
                        FROM albion.hole_section
                        WHERE st_dwithin(geom,
                            'SRID={srid} ;POINT({x} {y})'::geometry, 25)
                        ORDER by st_distance(
                            'SRID={srid} ;POINT({x} {y})'::geometry, geom)
                        LIMIT 1
                    """
                )
                res = cur.fetchone()

            return res[0] if res else None

    def add_named_section(self, section_id, geom):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute("SELECT srid FROM albion.metadata")
            (srid,) = cur.fetchone()
            cur.execute(
                """
                    INSERT INTO albion.named_section(geom, section)
                    VALUES (ST_SetSRID(%s::geometry, %s), %s)
                """,
                (geom.wkb_hex, srid, section_id)
            )
            con.commit()

    def set_section_geom(self, section_id, geom):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute("SELECT srid FROM albion.metadata")
            (srid,) = cur.fetchone()
            cur.execute(
                """
                    UPDATE albion.section
                    SET geom=st_multi(ST_SetSRID(%s::geometry, %s))
                    WHERE id=%s
                """,
                (geom.wkb_hex, srid, section_id)
            )
            con.commit()

    def check_graph_type(self, graph_id, graph_type):
        """Check if the provided graph is compliant with the provided type.

        If the graph has no type, set it to graph_type.

        Parameters
        ----------
        graph_id : str
            Name of the graph to consider
        graph_type : str
            Candidate graph type

        Returns
        -------
        bool
            True if the graph type is compliant with the graph, False otherwise
        """
        logger.info("Checking if graph %s is of type %s...", graph_id, graph_type)
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(
                """
                    SELECT graph_type FROM albion.graph
                    WHERE id=%s
                """,
                (graph_id,)
            )
            ret = cur.fetchone()[0]
            if ret is None:
                logger.info("No type, set to %s.", graph_type)
                cur.execute(
                    """
                        UPDATE albion.graph
                        SET graph_type=%s
                        WHERE id=%s
                    """,
                    (graph_type, graph_id)
                )
                return True
            else:
                logger.info(
                    "This graph has a defined type. Is the candidate type valid? %s",
                    ret == graph_type,
                )
                return ret == graph_type

    def add_to_graph_node(self, graph_id, features):
        with self.connect() as con:
            cur = con.cursor()
            cur.executemany(
                """
                    INSERT INTO
                    albion.node(from_, to_, hole_id, graph_id)
                    VALUES(%s, %s, %s, %s)
                """,
                [(f["from_"], f["to_"], f["hole_id"], graph_id) for f in features],
            )

    def accept_possible_edge(self, graph_id):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute(
                """
                INSERT INTO albion.edge(start_, end_, graph_id, geom)
                SELECT start_, end_, graph_id, geom FROM albion.possible_edge
                WHERE graph_id=%s
                """,
                (graph_id,),
            )

    def create_grid(self):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute("DROP TABLE IF EXISTS _albion.grid")
            cur.execute(
                """
                    CREATE TABLE _albion.grid AS (
                    SELECT id, geom FROM ST_CreateRegularGrid());
                """
            )
            cur.execute(
                """
                    ALTER TABLE _albion.grid
                    ADD CONSTRAINT albion_grid_pkey PRIMARY KEY (id);
                """
            )
            cur.execute(
                """
                    CREATE INDEX sidx_grid_geom ON _albion.grid
                    USING gist (geom);
                """
            )
            con.commit()

    def create_raster_from_formation(self, code, level, outDir):
        if not self.__has_grid():
            self.create_grid()

        with self.connect() as con:
            cur = con.cursor()
            cur.execute("DROP TABLE IF EXISTS _albion.current_raster")
            cur.execute(
                f"""CREATE TABLE _albion.current_raster AS
                      ( WITH points AS
                         ( SELECT g.id,
                                  CASE
                                     WHEN ST_Intersects(g.geom, c.geom)
                                     THEN ST_Z(st_interpolate_from_tin(g.geom, c.geom))
                                     ELSE -9999::float
                                 END AS z,
                                 g.geom geom
                         FROM _albion.grid g,
                              _albion.cells c
                         WHERE c.code = {code}
                           AND c.lvl = '{level}'
                           AND ST_Intersects(g.geom, c.geom)) SELECT *
                      FROM points
                      UNION SELECT g.id,
                                   -9999::float z,
                                   g.geom geom
                      FROM _albion.grid g
                      WHERE id NOT IN
                          (SELECT id
                           FROM points));
                """
            )
            con.commit()
            self.__export_raster(outDir, "z")

            cur.execute("DROP TABLE IF EXISTS _albion.current_raster")
            con.commit()

    def create_raster_from_collar(self, isDepth, outDir):
        if not self.__has_grid():
            self.create_grid()

        with self.connect() as con:
            cur = con.cursor()
            cur.execute("DROP TABLE IF EXISTS _albion.collar_cell")
            cur.execute("DROP TABLE IF EXISTS _albion.current_raster")
            cur.execute(
                """
                    CREATE TABLE _albion.collar_cell AS SELECT id,
                    ST_SetSRID(geom, (SELECT srid FROM _albion.metadata)) geom
                    FROM _albion.collar_cell(%s)
                """,
                (isDepth,)
            )
            cur.execute(
                """
                    CREATE TABLE _albion.current_raster AS
                       ( WITH points AS
                          ( SELECT g.id,
                                  CASE
                                     WHEN ST_Intersects(g.geom, C.geom)
                                     THEN ST_Z(st_interpolate_from_tin(g.geom, C.geom))
                                    ELSE -9999::FLOAT
                                  END AS val,
                                  g.geom geom
                         FROM _albion.grid g,
                                _albion.collar_cell C
                          WHERE  ST_Intersects(g.geom, C.geom)) SELECT *
                       FROM points
                        UNION SELECT g.id,
                                     -9999::FLOAT val,
                                     g.geom geom
                       FROM _albion.grid g
                       WHERE id NOT IN
                            (SELECT id
                             FROM points));
                """
            )
            con.commit()
            self.__export_raster(outDir, "val")
            cur.execute("DROP TABLE IF EXISTS _albion.current_raster")
            cur.execute("DROP TABLE IF EXISTS _albion.collar_cell")
            con.commit()

    def __x_spacing(self):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute("SELECT xspacing FROM _albion.metadata")
            return cur.fetchone()[0]

    def __y_spacing(self):
        with self.connect() as con:
            cur = con.cursor()
            cur.execute("SELECT yspacing FROM _albion.metadata")
            return cur.fetchone()[0]

    def __export_raster(self, outDir, field):
        x_spacing = self.__x_spacing()
        y_spacing = self.__y_spacing()

        with self.connect() as con:
            uri = QgsDataSourceUri()
            uri.setConnection(
                con.info.host,
                str(con.info.port),
                con.info.dbname,
                con.info.user,
                con.info.password,
            )
            uri.setDataSource("_albion", "current_raster", "geom")
            uri.setParam("checkPrimaryKeyUnicity", "0")
            uri.setSrid("32632")
            uri.setWkbType(QgsWkbTypes.Point)
            v = QgsVectorLayer(uri.uri(), "current_raster", "postgres")
            res = processing.run(
                "gdal:rasterize",
                {
                    "INPUT": v,
                    "FIELD": field,
                    "BURN": 0,
                    "UNITS": 1,
                    "WIDTH": x_spacing,
                    "HEIGHT": y_spacing,
                    "EXTENT": v.extent(),
                    "NODATA": -9999,
                    "OPTIONS": "",
                    "DATA_TYPE": 5,
                    "INIT": None,
                    "INVERT": False,
                    "EXTRA": "",
                    "OUTPUT": os.path.join(outDir, "dem.tif"),
                },
            )
            processing.run(
                "qgis:slope",
                {
                    "INPUT": res["OUTPUT"],
                    "Z_FACTOR": 1,
                    "OUTPUT": os.path.join(outDir, "slope.tif"),
                },
            )
            processing.run(
                "qgis:aspect",
                {
                    "INPUT": res["OUTPUT"],
                    "Z_FACTOR": 1,
                    "OUTPUT": os.path.join(outDir, "aspect.tif"),
                },
            )
            processing.run(
                "qgis:ruggednessindex",
                {
                    "INPUT": res["OUTPUT"],
                    "Z_FACTOR": 1,
                    "OUTPUT": os.path.join(outDir, "ruggednessindex.tif"),
                },
            )
