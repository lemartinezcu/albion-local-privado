# coding = utf-8
from qgis.PyQt.QtWidgets import QDialog
from qgis.PyQt import uic
import os


class MineralizationDialog(QDialog):
    def __init__(self, project, parent=None):
        QDialog.__init__(self, parent)
        uic.loadUi(os.path.join(os.path.dirname(__file__), "mineralization.ui"), self)
        self.__project = project

    def accept(self):
        self.__project.compute_mineralization(
            self.cutoff.value(), self.ci.value(), self.oc.value()
        )
        self.close()


if __name__ == "__main__":
    from qgis.PyQt.QtWidgets import QApplication
    import sys
    from albion.project import Project

    app = QApplication(sys.argv)
    MineralizationDialog(Project(sys.argv[1])).exec_()
