"""
Update consensus attribute table.
"""

import logging

from qgis.PyQt.QtWidgets import QDialog, QDialogButtonBox, QVBoxLayout

from qgis.gui import QgsDualView

logger = logging.getLogger(__name__)


class UpdateConsensusDialog(QDialog):
    """Dialog to update the consensus table"""

    def __init__(self, project, iface, layer, column_name, parent=None):
        QDialog.__init__(self, parent)
        self.__project = project
        self.__iface = iface
        self.__layer = layer
        self.__column_name = column_name
        self.__columns_states = []
        self.__initGui()
        self.__layer.startEditing()

    def __initGui(self):
        self.setWindowTitle("Set consensus depths before updating the markers...")
        self.__layout = QVBoxLayout(self)

        self.__consensusTable = QgsDualView()
        self.__initTable()

        self.__buttons = QDialogButtonBox(
            QDialogButtonBox.Save | QDialogButtonBox.Cancel
        )
        self.__layout.addWidget(self.__consensusTable)
        self.__layout.addWidget(self.__buttons)

        self.__buttons.rejected.connect(self.__reject)
        self.__buttons.accepted.connect(self.__accept)

    def __initTable(self):
        self.__consensusTable.init(self.__layer, self.__iface.mapCanvas())
        self.__consensusTable.setView(QgsDualView.AttributeTable)
        tableConfig = self.__layer.attributeTableConfig()
        tableConfigColumns = tableConfig.columns()
        for c in tableConfigColumns:
            self.__columns_states.append(c.hidden)
            c.hidden = c.name != self.__column_name
        tableConfig.setColumns(tableConfigColumns)
        self.__consensusTable.setAttributeTableConfig(tableConfig)
        self.__layer.setAttributeTableConfig(tableConfig)

    def __finish(self):
        tableConfig = self.__layer.attributeTableConfig()
        tableConfigColumns = tableConfig.columns()
        for i, c in enumerate(tableConfigColumns):
            c.hidden = self.__columns_states[i]
        tableConfig.setColumns(tableConfigColumns)
        self.__consensusTable.setAttributeTableConfig(tableConfig)
        self.__layer.setAttributeTableConfig(tableConfig)
        self.close()

    def __accept(self):
        logger.info("Update Similog computation...")
        ret = self.__layer.commitChanges()
        if not ret:
            self.__iface.messageBar().pushWarning(
                "Update consensus:", "Can not commit changes"
            )
        else:
            self.__iface.messageBar().pushInfo(
                "Update consensus:", "Update in progress"
            )
            # update Similog markers
            self.__project.update_similog_markers()
        self.__finish()

    def __reject(self):
        self.__layer.rollBack()
        self.__layer.commitChanges()
        self.__finish()
