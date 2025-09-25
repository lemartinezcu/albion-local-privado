"""Design the similog Dialog, so as to parametrize the consensus computation algorithm, using FAMSA
"""

import os
from functools import partial
import logging
import subprocess

import numpy as np
import pandas as pd

from qgis.PyQt.QtWidgets import QDialog
from qgis.PyQt import uic

try:
    from albion_similog import well_correlation
except ModuleNotFoundError:
    pass


logger = logging.getLogger(__name__)


def check_similog_dependency():
    """Check if the albion_similog Python dependency is installed on the computer."""
    try:
        import albion_similog

        logger.debug("Version of Similog: %s", albion_similog.__version__)
    except ModuleNotFoundError:
        return False
    return True


def check_famsa_dependency():
    """Check if the FAMSA dependency is installed on the computer."""
    try:
        subprocess.call(["famsa"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except FileNotFoundError:  # No famsa file in the current directory
        return False
    except PermissionError:  # A famsa file that is not executable
        return False
    except OSError:  # The famsa file is not really the expected program
        return False
    return True


class SimilogDialog(QDialog):
    """Dialog for setting the similog algorithm"""

    def __init__(self, project, parent=None):
        QDialog.__init__(self, parent)
        uic.loadUi(os.path.join(os.path.dirname(__file__), "similog.ui"), self)
        self.__project = project
        self.log_normalization = False
        self.wcorr = None
        self.markers = pd.DataFrame({"hole_id": [], "from": [], "to": [], "code": []})
        self._depth_min.valueChanged.connect(
            partial(self.__spin_box_constraints, self._depth_min, self._depth_max)
        )
        self._depth_max.valueChanged.connect(
            partial(self.__spin_box_constraints, self._depth_min, self._depth_max)
        )
        self._value_min.valueChanged.connect(
            partial(self.__spin_box_constraints, self._value_min, self._value_max)
        )
        self._value_max.valueChanged.connect(
            partial(self.__spin_box_constraints, self._value_min, self._value_max)
        )
        self._similog_progress.setValue(0)

    def __spin_box_constraints(self, min_spin_box, max_spin_box):
        if min_spin_box.value() > max_spin_box.value():
            min_spin_box.setValue(max_spin_box.value())

    def _exec(self):
        self.log_normalization = self._log_normalize.isChecked()
        self._dialog_button.setEnabled(False)
        self._similog_info.setText(
            "Click on 'Compute' button to run similog procedure..."
        )
        self._similog_progress.setValue(0)
        logger.info("Compute Similog resistivity...")
        with self.__project.connect() as con:
            # Read the data into the database
            logger.info("Read the resistivity data...")
            self._similog_info.setText("Read the resistivity data...")
            self._similog_progress.setValue(5)
            df = pd.read_sql(
                f"SELECT hole_id, from_, rho FROM {self._table.text()}", con
            )
            # Compute similog outputs
            df.reset_index(drop=True, inplace=True)
            logger.info("Create Similog object...")
            self._similog_info.setText("Create Similog object...")
            self._similog_progress.setValue(10)
            self.wcorr = well_correlation.WellCorrelation(
                df,
                match_column="rho",
                depth_column="from_",
                well_column="hole_id",
                min_seg=int(self._min_seg.value()),
                nb_markers=self._nb_markers.value(),
                depth_min=self._depth_min.value(),
                depth_max=self._depth_max.value(),
                value_min=self._value_min.value(),
                value_max=self._value_max.value(),
                log_normalize=self._log_normalize.isChecked(),
                lr_normalize=self._transcribe_regressed_values.isChecked(),
                segmentize_with_pelt=self._segmentize_with_pelt.isChecked(),
            )
            logger.info("Compute Similog outputs...")
            self._similog_info.setText("Compute Similog outputs...")
            self._similog_progress.setValue(50)
            self.wcorr.run()
            self._similog_info.setText("Compute optimal markers...")
            self._similog_progress.setValue(90)
            self.markers = well_correlation.compute_markers(
                self.wcorr.consensus_depth,
                self.wcorr.depth_match_global,
                self.wcorr.consensus,
                "from_",
            )
        self._similog_info.setText("Similog computation is OK!")
        self._similog_progress.setValue(100)
        self._dialog_button.setEnabled(True)

    def accept(self):
        logger.info("Accept Similog computation, waiting for insertion in base...")
        self.__project.add_similog_tables()
        consensus = self.wcorr.consensus.copy()
        consensus_markers = self.wcorr.consensus.loc[
            self.wcorr.consensus["from_"].isin(self.wcorr.consensus_depth)
        ]
        if self.log_normalization:
            consensus.loc[:, "rho"] = consensus.loc[:, "rho"].apply(np.exp)
            consensus_markers.loc[:, "rho"] = consensus_markers.loc[:, "rho"].apply(
                np.exp
            )
        self.__project.insert_similog_consensus(consensus, "similog_consensus")
        self.__project.insert_similog_depths(self.wcorr.depth_match_global)
        self.__project.insert_similog_consensus(
            consensus_markers, "similog_consensus_marker"
        )
        self.__project.insert_similog_markers(self.markers)
        self.close()
