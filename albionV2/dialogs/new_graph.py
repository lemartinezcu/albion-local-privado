"""New graph dialog
"""

import os

from qgis.PyQt.QtWidgets import QDialog
from qgis.PyQt import uic
from qgis.PyQt.QtWidgets import QAbstractItemView


class NewGraphDialog(QDialog):
    """Dialog for Albion new graphs, that needs to be named and characterized with a set of parent
    graphs, if relevant.

    """

    def __init__(self, existing_graphs, parent=None):
        QDialog.__init__(self, parent)
        uic.loadUi(os.path.join(os.path.dirname(__file__), "new_graph.ui"), self)
        self.parent_graph_widget.setSelectionMode(QAbstractItemView.MultiSelection)
        for g in existing_graphs:
            print(f"Add graph {g} to the list of possible parent...")
            self.parent_graph_widget.addItem(g)
