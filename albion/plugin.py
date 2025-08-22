import math
import os
import tempfile
import zipfile
from pathlib import Path, PurePath

import numpy as np
from qgis.PyQt.QtCore import QObject, QUrl, Qt
from qgis.PyQt.QtGui import QDesktopServices, QIcon, QKeySequence
from qgis.PyQt.QtWidgets import (
    QApplication, QComboBox, QFileDialog, QInputDialog, QLineEdit,
    QMenu, QMessageBox, QProgressBar, QShortcut, QToolBar,
)
from qgis.core import QgsDataSourceUri, QgsProject
from shapely.geometry import LineString

from albion.dialogs.export_elementary_volume import ExportElementaryVolume
from albion.dialogs.export_raster_collar import ExportRasterCollarDialog
from albion.dialogs.export_raster_formation import ExportRasterFormationDialog
from albion.dialogs.mineralization import MineralizationDialog
from albion.dialogs.new_graph import NewGraphDialog
from .project import ProgressBar, Project, find_in_dir
from .similog.update_consensus import UpdateConsensusDialog

try:
    from .similog.similog import (
        check_famsa_dependency,
        check_similog_dependency,
        SimilogDialog,
    )
except ModuleNotFoundError:
    HAVE_SIMILOG = False
else:
    HAVE_SIMILOG = True


def icon(name):
    """Return a QIcon instance from the `icons` directory"""
    path = PurePath(__file__).parent / "icons" / name
    return QIcon(str(path))


class Plugin(QObject):
    def __init__(self, iface):
        QObject.__init__(self)
        self.__iface = iface
        self.__shortcuts = []
        self.__current_section = QComboBox()
        self.__current_section.setMinimumWidth(150)
        self.__current_graph = QComboBox()
        self.__current_graph.setMinimumWidth(150)
        self.__toolbar = None
        self.__menu = None
        self.__log_strati = None
        self.__similog_dialog = None

    def initGui(self):
        for keyseq, slot in (
            (Qt.CTRL + Qt.ALT + Qt.Key_K, self.__create_group),
            (Qt.CTRL + Qt.ALT + Qt.Key_N, self.__next_section),
            (Qt.CTRL + Qt.ALT + Qt.Key_B, self.__previous_section),
            (Qt.CTRL + Qt.ALT + Qt.Key_J, self.__add_section_from_selection),
        ):

            short = QShortcut(QKeySequence(keyseq), self.__iface.mainWindow())
            short.setContext(Qt.ApplicationShortcut)
            short.activated.connect(slot)
            self.__shortcuts.append(short)

        self.__menu = QMenu("Albion")
        self.__menu.aboutToShow.connect(self.__create_menu_entries)
        self.__iface.mainWindow().menuBar().addMenu(self.__menu)

        self.__toolbar = QToolBar("Albion")
        self.__iface.addToolBar(self.__toolbar)

        self.__toolbar.addWidget(self.__current_graph)

        self.__toolbar.addWidget(self.__current_section)
        self.__current_section.currentIndexChanged[str].connect(
            self.__current_section_changed
        )

        self.__toolbar.addAction(
            icon("previous_line_big.svg"), "previous section  (Ctrl+Alt+b)"
        ).triggered.connect(self.__previous_section)

        self.__toolbar.addAction(
            icon("previous_line.svg"), "previous sub section"
        ).triggered.connect(self.__previous_subsection)

        self.__toolbar.addAction(
            icon("next_line.svg"), "next sub section"
        ).triggered.connect(self.__next_subsection)

        self.__toolbar.addAction(
            icon("next_line_big.svg"), "next section (Ctrl+Alt+n)"
        ).triggered.connect(self.__next_section)

        self.__toolbar.addAction(
            icon("line_from_selected.svg"), "create temporary section"
        ).triggered.connect(self.__section_from_selection)

        QgsProject.instance().readProject.connect(self.__qgis__project__loaded)
        self.__qgis__project__loaded()  # case of reload

    def unload(self):
        for shortcut in self.__shortcuts:
            shortcut.setParent(None)
        self.__toolbar and self.__toolbar.setParent(None)
        self.__menu and self.__menu.setParent(None)

    def __add_menu_entry(self, name, callback, enabled=True, help_str=""):
        act = self.__menu.addAction(name)
        if callback is not None:
            act.triggered.connect(callback)
            act.setEnabled(enabled)
            act.setToolTip(help_str)
        else:
            act.setEnabled(False)
            act.setToolTip("NOT INMPLEMENTED " + help_str)
        return act

    def __create_menu_entries(self):

        self.__menu.clear()

        self.__add_menu_entry("New &Project", self.__new_project)

        self.__add_menu_entry("Import Project", self.__import_project)

        self.__add_menu_entry(
            "Export Project", self.__export_project, self.project is not None
        )

        self.__add_menu_entry("Upgrade Project", self.__upgrade_project)

        self.__menu.addSeparator()

        self.__add_menu_entry(
            "&Import directory",
            self.__import_data,
            self.project is not None,
            "Import data from directory",
        )

        self.__add_menu_entry(
            "Add data",
            self.__import_new_data,
            self.project is not None
            and self.project.has_hole,
            "Add data from directory",
        )

        # should be removed?
        self.__add_menu_entry(
            "&Import holes",
            None,  # self.__import_holes,
            self.project is not None and False,
            "Import hole data from directory",
        )

        self.__add_menu_entry(
            "Export holes",
            self.__export_holes,
            self.project is not None and self.project.has_hole,
            "Export hole trace in .vtk or .dxf format",
        )

        self.__add_menu_entry(
            "Import layer",
            self.__import_layer,
            self.project is not None,
            "Import data from selected layer.",
        )

        self.__add_menu_entry(
            "Export layer", self.__export_layer, self.project is not None
        )

        self.__add_menu_entry(
            "Compute &Mineralization",
            self.__compute_mineralization,
            self.project is not None and self.project.has_radiometry,
            "",
        )
        self.__add_menu_entry(
            "Compute similog resistivity",
            self.__compute_similog_resistivity,
            (
                self.project is not None
                and HAVE_SIMILOG
                and self.project.has_resistivity_section
                and check_similog_dependency()
                and check_famsa_dependency()
            ),
            "Compute similog resisistivity with FAMSA algorithm",
        )
        self.__add_menu_entry(
            "Update similog markers",
            self.__update_similog_markers,
            (
                self.project is not None
                and HAVE_SIMILOG
                and self.project.has_resistivity_section
                and check_similog_dependency()
                and check_famsa_dependency()
                and self.project.has_similog_consensus
            ),
            "Update similog markers with consensus marker depths",
        )
        self.__menu.addSeparator()

        self.__menu.addSeparator()

        self.__add_menu_entry(
            "Create cells",
            self.__create_cells,
            self.project is not None and self.project.has_hole,
            "Create Delaunay triangulation of collar layer.",
        )

        self.__add_menu_entry(
            "Create subsections",
            self.__create_sections,
            self.project is not None and self.project.has_group_cell,
            "Once cell groups have been defined, create section lines.",
        )

        self.__add_menu_entry(
            "Refresh selected layers sections",
            self.__refresh_selected_layers_sections,
            self.project is not None,
            "",
        )

        self.__menu.addSeparator()

        self.__add_menu_entry(
            "New &Graph",
            self.__new_graph,
            self.project is not None,
            "Create a new graph",
        )

        self.__add_menu_entry(
            "Delete Graph",
            self.__delete_graph,
            self.project is not None and self.project.has_graph,
        )

        self.__add_menu_entry(
            "Add selection to graph nodes",
            self.__add_selection_to_graph_node,
            self.project is not None and self.project.has_graph,
        )

        self.__add_menu_entry(
            "Accept graph possible edges",
            self.__accept_possible_edge,
            self.project is not None and self.project.has_graph,
        )

        self.__add_menu_entry(
            "Create terminations",
            self.__create_terminations,
            self.project is not None and bool(self.__current_graph.currentText()),
            "Create terminations associated with current graph.",
        )

        self.__menu.addSeparator()

        self.__add_menu_entry(
            "Create volumes",
            self.__create_volumes,
            self.project is not None and bool(self.__current_graph.currentText()),
            "Create volumes associated with current graph.",
        )

        self.__add_menu_entry(
            "Export Volume",
            self.__export_volume,
            self.project is not None and bool(self.__current_graph.currentText()),
            "Export volume of current graph in .obj or .dxf format",
        )

        self.__add_menu_entry(
            "Export Elementary Volume",
            self.__export_elementary_volume,
            self.project is not None and bool(self.__current_graph.currentText()),
            "Export an elementary volume of current graph in .obj or .dxf format",
        )

        self.__add_menu_entry(
            "Export Sections",
            self.__export_sections,
            self.project is not None
            and bool(self.__current_graph.currentText())
            and self.project.has_section
            and self.project.has_volume,
            "Export triangulated section in .obj or .dxf format",
        )

        self.__add_menu_entry(
            "(Re)create grid points for raster export",
            self.__export_create_grid,
            self.project is not None and self.project.has_cell,
            """(re)create grid points for raster export. Only needed if xspacing
            and yspacing metadata changed""",
        )
        self.__add_menu_entry(
            "Export rasters from formation",
            self.__export_raster_formation,
            self.project is not None and self.project.has_cell,
            "Export rasters (DEM, aspect, slope, ruggedness index) from formation",
        )

        self.__add_menu_entry(
            "Export rasters from collar",
            self.__export_raster_collar,
            self.project is not None and self.project.has_cell,
            "Export rasters (DEM, aspect, slope, ruggedness index) from collar",
        )

        self.__menu.addSeparator()

        self.__menu.addAction("Help").triggered.connect(self.open_help)

    def __getattr__(self, name):
        if name == "project":
            project_name = QgsProject.instance().readEntry(
                "albion", "project_name", ""
            )[0]
            return Project(project_name) if project_name else None

        return super().__getattr__(name)

    def __create_terminations(self):
        self.project.create_terminations(self.__current_graph.currentText())
        self.__refresh_layers("section")

    def __create_volumes(self):
        self.project.create_volumes(self.__current_graph.currentText())

    def __next_section(self):
        self.project.next_section(self.__current_section.currentText())
        self.__refresh_layers("section")

    def __previous_section(self):
        self.project.previous_section(self.__current_section.currentText())
        self.__refresh_layers("section")

    def __next_subsection(self):
        self.project.next_subsection(self.__current_section.currentText())
        self.__refresh_layers("section")

    def __previous_subsection(self):
        self.project.previous_subsection(self.__current_section.currentText())
        self.__refresh_layers("section")

    def __refresh_layers(self, name=None):
        for layer in self.__iface.mapCanvas().layers():
            if name is None or layer.name().find(name) != -1:
                layer.triggerRepaint()

    def __layer(self, name):
        project_layers = self.__iface.mapCanvas().layers()
        if not name:
            return project_layers[-1]

        for layer in project_layers:
            if layer.name() == name:
                return layer

        return None

    def __current_section_changed(self, section_id):
        layers = QgsProject.instance().mapLayersByName(u"group_cell")
        if layers:
            layers[0].setSubsetString(f"section_id='{section_id}'")
        self.__refresh_layers("section")

    def __create_group(self):
        if self.__iface.activeLayer() and self.__iface.activeLayer().name() == u"cell":
            if self.__iface.activeLayer().selectedFeatureCount():
                section = self.__current_section.currentText()
                self.project.create_group(
                    section,
                    [f["id"] for f in self.__iface.activeLayer().selectedFeatures()],
                )
            self.__iface.activeLayer().removeSelection()
            self.__refresh_layers("group_cell")

    def __qgis__project__loaded(self):
        if self.project is None:
            return

        self.__current_graph.clear()
        self.__current_section.clear()
        self.__current_section.addItems(self.project.sections())
        self.__current_graph.addItems(self.project.graphs())

        layers = QgsProject.instance().mapLayersByName("section.anchor")
        if layers:
            layers[0].editingStopped.connect(self.__update_section_list)

        # We make sure that corresponding extents are valid when the project
        # is loaded
        cell = QgsProject.instance().mapLayersByName("cell")
        if cell:
            cell[0].updateExtents()

        section_geom = QgsProject.instance().mapLayersByName("section.geom")
        if section_geom:
            section_geom[0].updateExtents()

    def __update_section_list(self):
        self.__current_section.clear()
        self.__current_section.addItems(self.project.sections())

    def __upgrade_project(self):
        project_name, ok = QInputDialog.getText(
            self.__iface.mainWindow(),
            "Database name",
            "Database name:",
            QLineEdit.Normal,
            "",
        )
        if not ok:
            return

        project = Project(project_name)
        project.update()
        QgsProject.instance().writeEntry("albion", "project_name", project.name)
        QgsProject.instance().writeEntry("albion", "srid", project.srid)
        self.__qgis__project__loaded()

    def __new_project(self):
        filename, _ = QFileDialog.getSaveFileName(
            None,
            u"New project name (no space, plain ascii)",
            QgsProject.instance().readEntry("albion", "last_dir", "")[0],
            "QGIS poject file (*.qgs)",
        )
        if not filename:
            return

        if not filename.endswith(".qgs"):
            filename += ".qgs"
        filename = filename.replace(" ", "_")

        if not filename.isascii():
            self.__iface.messageBar().pushError(
                "Albion:", "project name may only contain ascii character (no accent)"
            )
            return

        srid, ok = QInputDialog.getText(
            self.__iface.mainWindow(),
            "Project SRID",
            "Project SRID EPSG:",
            QLineEdit.Normal,
            "32632",
        )
        if not ok:
            return

        srid = int(srid)

        project_path = Path(filename)
        project_name = project_path.stem

        if Project.exists(project_name):
            delete_db_message_box = QMessageBox(
                    QMessageBox.Information,
                    "Delete existing DB",
                    f"Database {project_name} exits, do you want to delete it ?",
                    QMessageBox.Yes | QMessageBox.No,
                )
            if delete_db_message_box.exec_() != QMessageBox.Yes:
                self.__iface.messageBar().pushInfo(
                    "Albion:", "keeping existing database..."
                )
            else:
                Project.delete(project_name)
                self.__iface.messageBar().pushInfo("Albion:", "creating project...")
                Project.create(project_name, srid)
        else:
            self.__iface.messageBar().pushInfo("Albion:", "creating project...")
            Project.create(project_name, srid)

        try:
            project_path.unlink()  # the option missing_ok was added in 3.8
        except FileNotFoundError:
            pass

        # load template
        template_path = Path(__file__).parent / "template_project.qgs"
        with template_path.open() as f:
            template = f.read()
            template = template.replace("%%PROJECT%%", project_name)
            template = template.replace("32632", str(srid))

        with project_path.open('w') as f:
            f.write(template)

        self.__iface.newProject()
        QgsProject.instance().setFileName(filename)
        QgsProject.instance().read()
        QgsProject.instance().writeEntry("albion", "project_name", project_name)
        QgsProject.instance().writeEntry("albion", "srid", srid)
        QgsProject.instance().write()
        self.__qgis__project__loaded()

    def __import_data(self):
        assert self.project
        directory = QFileDialog.getExistingDirectory(
            None,
            "Data directory",
            QgsProject.instance().readEntry("albion", "last_dir", "")[0],
            QFileDialog.ShowDirsOnly | QFileDialog.DontUseNativeDialog,
        )
        
        if not directory:
            return
        
        QgsProject.instance().writeEntry("albion", "last_dir", directory),

        progress_message_bar = self.__iface.messageBar().createMessage(
            f"Loading {directory}..."
        )
        progress = QProgressBar()
        progress.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        progress_message_bar.layout().addWidget(progress)
        self.__iface.messageBar().pushWidget(progress_message_bar)

        self.project.import_data(directory, ProgressBar(progress))
        # self.project.triangulate()
        self.project.create_section_view_0_90(4)

        self.__iface.messageBar().clearWidgets()

        collar = QgsProject.instance().mapLayersByName("collar")
        if collar:
            collar[0].reload()
            collar[0].updateExtents()
            self.__iface.setActiveLayer(collar[0])

            QApplication.instance().processEvents()
            while self.__iface.mapCanvas().isDrawing():
                QApplication.instance().processEvents()
            self.__iface.zoomToActiveLayer()

        self.__iface.actionSaveProject().trigger()

        self.__current_section.clear()
        self.__current_section.addItems(self.project.sections())

    def __import_new_data(self):
        assert self.project
        directory = QFileDialog.getExistingDirectory(
            None,
            "Data directory",
            QgsProject.instance().readEntry("albion", "last_dir", "")[0],
            QFileDialog.ShowDirsOnly | QFileDialog.DontUseNativeDialog,
        )

        if not directory:
            return

        QgsProject.instance().writeEntry("albion", "last_dir", directory),

        progress_message_bar = self.__iface.messageBar().createMessage(
            f"Loading {directory}..."
        )
        progress = QProgressBar()
        progress.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        progress_message_bar.layout().addWidget(progress)
        self.__iface.messageBar().pushWidget(progress_message_bar)

        self.project.import_data(directory, ProgressBar(progress))

        self.__iface.messageBar().clearWidgets()

        collar = QgsProject.instance().mapLayersByName("collar")
        if collar:
            collar[0].reload()
            collar[0].updateExtents()
            self.__iface.setActiveLayer(collar[0])

            QApplication.instance().processEvents()
            while self.__iface.mapCanvas().isDrawing():
                QApplication.instance().processEvents()
            self.__iface.zoomToActiveLayer()

        self.__iface.actionSaveProject().trigger()

        if self.project.has_cell:
            self.project.triangulate()
            self.__refresh_layers()

    def __import_layer(self):
        assert self.project
        if not self.__iface.activeLayer():
            return

        from_idx = None
        to_idx = None
        hole_id_idx = None
        other_idx = []
        definitions = []
        fields = []
        for idx, feature in enumerate(self.__iface.activeLayer().fields()):
            field_name = feature.name().lower()
            if field_name == "from" or field_name == "from_":
                from_idx = idx
            elif field_name == "to" or field_name == "to_":
                to_idx = idx
            elif field_name == "hole_id" or field_name == "holeid":
                hole_id_idx = idx
            else:
                other_idx.append(idx)
                name = field_name.replace(" ", "_")
                fields.append(name)
                type_ = "varchar"
                if feature.typeName() == "double":
                    type_ = "double precision"
                elif feature.typeName() == "integer":
                    type_ = "integer"
                definitions.append(name + " " + type_)

        table = {
            "NAME": self.__iface.activeLayer().name().lower().replace(" ", "_"),
            "FIELDS_DEFINITION": ", ".join(definitions),
            "FIELDS": ", ".join(fields),
            "SRID": self.project.srid,
        }

        if None in (from_idx, to_idx, hole_id_idx):
            self.__iface.messageBar().pushCritical(
                "Albion",
                "imported layer must have 'to', 'from' and 'hole_id' fields",
            )
            return

        values = []
        for feature in self.__iface.activeLayer().getFeatures():
            values.append(
                (feature[hole_id_idx], feature[from_idx], feature[to_idx])
                + tuple((feature[i] for i in other_idx))
            )
        self.project.add_table(table, values)

    def __new_graph(self):
        existing_graphs = self.project.get_existing_graphs()
        # Simple dialog with a text area for naming the new graph
        # and a list widget for declaring parent graphs.
        dlg = NewGraphDialog(existing_graphs)
        if dlg.exec_():
            graph = dlg.graph_name.text()
            reference_items = dlg.parent_graph_widget.selectedItems()
            references = [ri.text() for ri in reference_items]
            self.project.new_graph(graph, references)
            self.__current_graph.addItem(graph)
            self.__current_graph.setCurrentIndex(self.__current_graph.findText(graph))

    def __delete_graph(self):
        graph, ok = QInputDialog.getText(
            self.__iface.mainWindow(),
            "Graph",
            "Graph name:",
            QLineEdit.Normal,
            self.__current_graph.currentText(),
        )

        if not ok:
            return

        self.__current_graph.removeItem(self.__current_graph.findText(graph))
        self.project.delete_graph(graph)

    def __add_selection_to_graph_node(self):
        assert self.project
        # TODO ADD DIALOG TO REMIND USER THE CURRENT GRAPH
        if self.__iface.activeLayer() and self.__iface.activeLayer().selectedFeatures():
            selection = self.__iface.activeLayer().selectedFeatures()
            graph = self.__current_graph.currentText()

            adding_edge_message_box = QMessageBox(
                    QMessageBox.Information,
                    "Adding selected edges",
                    f"Do you want to add {len(selection)} selected edges to {graph} ?",
                    QMessageBox.Yes | QMessageBox.No,
                )
            if adding_edge_message_box.exec_() != QMessageBox.Yes:
                return

            # Basically the type is the active layer name without the "_section" suffix
            data_type = "_".join(self.__iface.activeLayer().name().split("_")[:-1])
            if self.project.check_graph_type(graph, data_type):
                self.project.add_to_graph_node(graph, selection)
            else:
                self.__iface.messageBar().pushMessage(
                    f"{data_type} is not a valid graph type for {graph}, aborting.",
                    level=1,
                )

        self.__refresh_layers()

    def __accept_possible_edge(self):
        assert self.project
        self.project.accept_possible_edge(self.__current_graph.currentText())

    def __create_cells(self):
        assert self.project

        if self.project.has_cell:
            creating_cell_message_box = QMessageBox(
                QMessageBox.Information,
                "Creating cells",
                "Do you want to replace project cells (your graphs will become invalid) ?",
                QMessageBox.Yes | QMessageBox.No,
            )
            if creating_cell_message_box.exec_() != QMessageBox.Yes:
                return

        self.project.triangulate()
        self.__refresh_layers()

    def __create_sections(self):
        assert self.project
        self.project.create_sections()

    def __refresh_selected_layers_sections(self):
        assert self.project
        for layer in self.__iface.layerTreeView().selectedLayers():
            uri = QgsDataSourceUri(layer.dataProvider().dataSourceUri())
            table = uri.table()
            if table.endswith("_section"):
                table = table[:-8]
            self.project.refresh_section_geom(table)
            self.__refresh_layers(table + "_section")

    def __compute_mineralization(self):
        MineralizationDialog(self.project).exec_()

    def __compute_similog_resistivity(self):
        if self.__similog_dialog is None:
            self.__similog_dialog = SimilogDialog(self.project)
        self.__similog_dialog.exec_()

    def __update_similog_markers(self):
        UpdateConsensusDialog(
            self.project,
            self.__iface,
            self.__layer("similog_consensus_marker"),
            "from_",
        ).exec_()

    def __export_volume(self):
        assert self.project

        filename, _ = QFileDialog.getSaveFileName(
            None,
            "Export volume for current graph",
            QgsProject.instance().readEntry("albion", "last_dir", "")[0],
            "File formats (*.dxf *.obj)",
        )
        if not filename:
            return

        pathname = PurePath(filename)
        QgsProject.instance().writeEntry("albion", "last_dir", str(pathname.parent))

        if pathname.suffix == ".obj":
            self.project.export_volume_obj(self.__current_graph.currentText(), str(pathname))
        elif pathname.suffix == ".dxf":
            self.project.export_volume_dxf(self.__current_graph.currentText(), str(pathname))
        else:
            self.__iface.messageBar().pushWarning(
                "Albion", "unsupported extension for volume export"
            )

    def __export_elementary_volume(self):
        assert self.project

        layer = self.__layer("cell")
        if not layer:
            self.__iface.messageBar().pushWarning(
                "Albion", "cell layer must be selected"
            )
            return

        graph = self.__current_graph.currentText()
        export_widget = ExportElementaryVolume(layer, self.project, graph)
        export_widget.show()
        export_widget.exec_()

    def __export_sections(self):
        assert self.project

        filename, _ = QFileDialog.getSaveFileName(
            None,
            "Export named sections for current graph",
            QgsProject.instance().readEntry("albion", "last_dir", "")[0],
            "File formats (*.dxf *.obj)",
        )
        if not filename:
            return

        pathname = PurePath(filename)
        QgsProject.instance().writeEntry("albion", "last_dir", str(pathname.parent))

        if pathname.suffix == ".obj":
            self.project.export_sections_obj(self.__current_graph.currentText(), str(pathname))
        elif pathname.suffix == ".dxf":
            self.project.export_sections_dxf(self.__current_graph.currentText(), str(pathname))
        else:
            self.__iface.messageBar().pushWarning(
                "Albion", "unsupported extension for section export"
            )

    def __export_holes(self):
        assert self.project

        filename, _ = QFileDialog.getSaveFileName(
            None,
            "Export holes",
            QgsProject.instance().readEntry("albion", "last_dir", "")[0],
            "File formats (*.dxf *.vtk)",
        )
        if not filename:
            return

        pathname = PurePath(filename)
        QgsProject.instance().writeEntry("albion", "last_dir", str(pathname.parent))

        if pathname.suffix == ".vtk":
            self.project.export_holes_vtk(filename)
        elif pathname.suffix == ".dxf":
            self.project.export_holes_dxf(filename)
        else:
            self.__iface.messageBar().pushWarning(
                "Albion", "unsupported extension for hole export"
            )

    def __export_layer(self):
        assert self.project

        table = None
        for layer in self.__iface.layerTreeView().selectedLayers():
            uri = QgsDataSourceUri(layer.dataProvider().dataSourceUri())
            table = uri.table()
            if table.endswith("_section"):
                table = table[:-8]
                break

        if table is None:
            self.__iface.messageBar().pushWarning("Albion", "you must select a layer")
            return

        filename, _ = QFileDialog.getSaveFileName(
            None,
            "Export layer",
            QgsProject.instance().readEntry("albion", "last_dir", "")[0],
            "File formats (*.dxf *.vtk)",
        )
        if not filename:
            return

        pathname = PurePath(filename)
        QgsProject.instance().writeEntry("albion", "last_dir", str(pathname.parent))

        if filename.endswith(".vtk"):
            self.project.export_layer_vtk(table, str(pathname))
        elif filename.endswith(".dxf"):
            self.project.export_layer_dxf(table, str(pathname))
        else:
            self.__iface.messageBar().pushWarning(
                "Albion", "unsupported extension for hole export"
            )

    def __import_project(self):
        filename, _ = QFileDialog.getOpenFileName(
            None,
            "Import project from file",
            QgsProject.instance().readEntry("albion", "last_dir", "")[0],
            "File formats (*.zip)",
        )
        if not filename:
            return

        pathname = PurePath(filename)
        QgsProject.instance().writeEntry("albion", "last_dir", str(pathname.parent)),

        if pathname.suffix != ".zip":
            self.__iface.messageBar().pushWarning(
                "Albion", "unsupported extension for import"
            )

        project_name = PurePath(filename).stem
        temp_directory = tempfile.mkdtemp()
        with zipfile.ZipFile(filename, "r") as z:
            z.extractall(temp_directory)

        dump = find_in_dir(temp_directory, ".dump")
        prj = find_in_dir(temp_directory, ".qgs")

        self.__iface.messageBar().pushInfo(
            "Albion", f"loading {project_name} from {dump}"
        )

        dbname = os.path.splitext(os.path.basename(dump))[0]

        if Project.exists(dbname):
            delete_existing_db_message_box = QMessageBox(
                    QMessageBox.Information,
                    "Delete existing DB",
                    f"Database {dbname} exits, to you want to delete it ?",
                    QMessageBox.Yes | QMessageBox.No,
                )

            if delete_existing_db_message_box.exec_() != QMessageBox.Yes:
                return

            Project.delete(dbname)

        Project.import_project(dbname, dump)
        QgsProject.instance().read(prj)

    def __export_project(self):
        if self.project is None:
            return

        filename, _ = QFileDialog.getSaveFileName(
            None,
            "Export project",
            QgsProject.instance().readEntry("albion", "last_dir", "")[0],
            "Data files(*.zip)",
        )
        if not filename:
            return

        pathname = Path(filename)
        QgsProject.instance().writeEntry("albion", "last_dir", str(pathname.parent)),

        try:
            pathname.unlink()  # the option missing_ok was added in 3.8
        except FileNotFoundError:
            pass

        with zipfile.ZipFile(str(pathname), "w") as project:
            dump = tempfile.mkstemp()[1]
            self.project.export_project(dump)
            project.write(dump, self.project.name + ".dump")
            project.write(
                QgsProject.instance().fileName(),
                os.path.split(QgsProject.instance().fileName())[1],
            )

    def __export_create_grid(self):
        if self.project is None:
            return

        self.project.create_grid()

    def __export_raster_formation(self):
        ExportRasterFormationDialog(self.project).exec_()

    def __export_raster_collar(self):
        ExportRasterCollarDialog(self.project).exec_()

    def __line_from_selection(self):
        if (
            self.__iface.activeLayer()
            and self.__iface.activeLayer().name() == u"collar"
            and self.__iface.activeLayer().selectedFeatures()
        ):
            collar = self.__iface.activeLayer()
            selection = collar.selectedFeatures()

            if len(selection) < 2:
                return

            def align(points):
                assert len(points) >= 2
                res = np.array(points[:2])
                for p in points[2:]:
                    u, v = res[0] - res[1], p - res[1]
                    if np.dot(u, v) < 0:
                        res[1] = p
                    elif np.dot(u, u) < np.dot(v, v):
                        res[0] = p
                # align with ref direction
                sqrt2 = math.sqrt(2.0) / 2
                points = points[
                    np.argsort(np.dot(points - res[0], res[1] - res[0]))
                ]
                d = np.array(points[-1] - points[0])
                dr = np.array([(0, 1), (sqrt2, sqrt2), (1, 0), (sqrt2, -sqrt2)])
                i = np.argmax(np.abs(dr.dot(d)))
                return points if (dr.dot(d))[i] > 0 else points[::-1]

            line = LineString(
                align(np.array([f.geometry().asPoint() for f in selection]))
            )
            collar.removeSelection()
            return line
        else:
            return None

    def __add_section_from_selection(self):
        assert self.project
        line = self.__line_from_selection()
        if line:
            self.project.add_named_section(self.__current_section.currentText(), line)
            self.__refresh_layers("named_section")

    def __section_from_selection(self):
        assert self.project
        line = self.__line_from_selection()
        if line:
            self.project.set_section_geom(self.__current_section.currentText(), line)
            self.__refresh_layers("section")

    def open_help(self):
        QDesktopServices.openUrl(
            QUrl.fromLocalFile(
                os.path.join(
                    os.path.dirname(__file__), "doc", "build", "html", "index.html"
                )
            )
        )
