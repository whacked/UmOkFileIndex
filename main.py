import sys, platform, os
import hashlib

from PyQt4.QtGui import QApplication
from PyQt4.QtCore import QDir, Qt
from PyQt4 import QtCore, QtGui

import Indexing as IX
from Indexing import utf8str
from sets import Set

IX.init_db()

PLATFORM_NAME = platform.system()
if PLATFORM_NAME == "Windows":
    OPEN_CMD = "start"
elif PLATFORM_NAME == "Darwin":
    OPEN_CMD = "open"
elif PLATFORM_NAME == "Linux":
    print "assuming xdg-open supported!"
    OPEN_CMD = "xdg-open"
else:
    print "cannot detect platform. open operation will not be supported"
    OPEN_CMD = None

class InstantSearchLineEdit(QtGui.QLineEdit):

    def __init__(self, parent, target_table):
        self.table = target_table
        super(QtGui.QLineEdit, self).__init__(parent)

    def keyPressEvent(self, event):
        QtGui.QLineEdit.keyPressEvent(self, event)

        ## use this if you want a fixed table size
        ## instead of dynamica resizing
        ## self.table.clearContents()
        while self.table.rowCount() > 0:
            self.table.removeRow(0)

        ## to address strange behavior (bug?)
        ## http://stackoverflow.com/questions/3498829/how-to-get-this-qtablewidget-to-display-items
        self.table.setSortingEnabled(False)
        ltoken = filter(lambda token: len(token)>1, unicode(self.displayText()).split())
        if not ltoken: return

        lres = IX.File.findall(IX.File.OP_AND, ltoken)
        for i, f in enumerate( lres ):
            self.table.insertRow(i)
            self.table.setItem(i, 0, QtGui.QTableWidgetItem(" ".join([utf8str(t.text) for t in f.taglist[:3]])))
            self.table.setItem(i, 1, QtGui.QTableWidgetItem(f.path))
            self.table.setItem(i, 2, QtGui.QTableWidgetItem(str(f.id)))
        self.table.setSortingEnabled(True)

class MainApp(QtGui.QWidget):
    def __init__(self, ROOT_DIR, parent=None):
        super(MainApp, self).__init__(parent)

        tv = self.tableView = QtGui.QTableWidget(0,3)
        tv.doubleClicked.connect(self.openFileCommand)
        tv.selectionModel().selectionChanged.connect(self.updateTagDisplayCommand)
        tv.horizontalHeader().setStretchLastSection(True)
        tv.setColumnHidden(2, True)
        for i, label in enumerate("tags file".split()):
            tv.setHorizontalHeaderItem(i, QtGui.QTableWidgetItem(label))
        ## disable editing
        tv.setEditTriggers( QtGui.QTableWidget.NoEditTriggers )

        self.searchInputLabel = QtGui.QLabel(self)
        self.searchInputLabel.setText("search:")
        self.searchInputEdit = InstantSearchLineEdit(self, self.tableView)
        self.searchInputEdit.setFocus(True)

        self.searchInputGrid = QtGui.QGridLayout()
        self.searchInputGrid.addWidget(self.searchInputLabel, 2, 0)
        self.searchInputGrid.addWidget(self.searchInputEdit, 2, 1)

        self.tagEditText     = QtGui.QPlainTextEdit(self)
        self.tagEditLabel    = QtGui.QLabel(self)
        self.tagEditLabel.setText("tags in item(s)")
        self.tagEditButton = QtGui.QPushButton('apply changes', self)
        self.tagEditButton.clicked.connect(self.applyTagEditCommand)

        self.hashInfoText       = QtGui.QLineEdit(self)
        self.hashInfoLabel      = QtGui.QLabel(self)
        self.hashInfoLabel.setText("checksum:")
        self.hashInfoVerifyButton = QtGui.QPushButton('verify', self)
        self.hashInfoVerifyButton.clicked.connect(self.verifyShaCommand)
        self.focusedFile = None

        self.filePathText       = QtGui.QLineEdit(self)
        self.filePathText.setReadOnly(True)
        self.filePathLabel      = QtGui.QLabel(self)
        self.filePathLabel.setText("file path")
        self.filePathOpenButton = QtGui.QPushButton('open parent dir', self)
        self.filePathOpenButton.clicked.connect(self.openDirCommand)

        self.fileInfoGrid = QtGui.QGridLayout()
        self.fileInfoGrid.setColumnMinimumWidth(0, 80)
        self.fileInfoGrid.addWidget(self.hashInfoLabel,        0, 0)
        self.fileInfoGrid.addWidget(self.hashInfoText,         0, 1)
        self.fileInfoGrid.addWidget(self.hashInfoVerifyButton, 0, 2)

        self.fileInfoGrid.addWidget(self.filePathLabel,        1, 0)
        self.fileInfoGrid.addWidget(self.filePathText,         1, 1)
        self.fileInfoGrid.addWidget(self.filePathOpenButton,   1, 2)

        self.layout = QtGui.QVBoxLayout(self)
        self.layout.addLayout(self.searchInputGrid)
        self.layout.addWidget(self.tableView)
        self.layout.addWidget(self.tagEditLabel)
        self.layout.addWidget(self.tagEditText)
        self.layout.addWidget(self.tagEditButton)
        self.layout.addLayout(self.fileInfoGrid)

        QtGui.QShortcut(QtGui.QKeySequence("Ctrl+Q"), self, self.close)

    def _system_open(self, filename):
        os.system(OPEN_CMD + " '" + filename.replace("'", "'\\''""'") + "'")

    def getFileAtRow(self, rowidx):
        # retrieve id from hidden data column (col id 2)
        tfile_id = int(self.tableView.item(rowidx, 2).text())
        return IX.File.get(id = tfile_id)

    def openDirCommand(self):
        self._system_open(os.path.split(self.focusedFile.get_realpath())[0])

    def getSelectedFileList(self):
        dfile = {}
        for idx in self.tableView.selectedIndexes():
            if idx.row() in dfile: continue
            dfile[idx.row()] = self.getFileAtRow(idx.row())
        return dfile

    def getSharedTagList(self, lfile):
        return reduce(lambda s, t: Set(s).intersection(t), [[utf8str(t.text) for t in f.taglist] for f in lfile])

    def updateTagDisplayCommand(self):
        dfile = self.getSelectedFileList()
        if not dfile:
            self.tagEditLabel.setText("nothing selected")
        elif len(dfile) is 1:
            f = dfile.values()[0]
            self.focusAndShowFileInfo(f)
            self.tagEditText.setPlainText(", ".join([utf8str(t.text) for t in f.taglist]))
            self.tagEditLabel.setText("tags in %s" % (f.path))
        else:
            ## show intersection of tags
            self.tagEditLabel.setText("shared tags among %d files" % len(dfile))
            self.tagEditText.setPlainText(", ".join(self.getSharedTagList(dfile.values())))

    def applyTagEditCommand(self):
        dfile = self.getSelectedFileList()
        if not dfile:
            return
        ltag_old = Set(self.getSharedTagList(dfile.values()))
        ltag_new = Set([text.strip() for text in unicode(self.tagEditText.toPlainText()).decode("utf-8").lower().strip(",").split(",")])

        ## ADD
        ltagadd = map(IX.Tag.guaranteed_get, ltag_new.difference(ltag_old))
        for f in dfile.values():
            f.add_tag(*ltagadd)

        ## DEL
        ltagdel = map(IX.Tag.guaranteed_get, ltag_old.difference(ltag_new))
        for f in dfile.values():
            f.del_tag(*ltagdel)


    def verifyShaCommand(self):
        expected = str(self.hashInfoText.text()).strip()
        measured = hashlib.sha1(open(self.focusedFile.get_realpath()).read()).hexdigest()

        if expected == measured:
            result_text = "matches"
            result_style = "{ background-color : green; color : white; }"
        else:
            result_text = "failed"
            result_style = "{ background-color : red; color : white; }"
        self.hashInfoLabel.setText(result_text)
        self.hashInfoText.setStyleSheet("QLineEdit "+result_style)

    def focusAndShowFileInfo(self, f):
        self.hashInfoText.setText(f.sha1)
        self.focusedFile = f

        self.hashInfoLabel.setText("checksum:")
        self.hashInfoText.setStyleSheet("")

        self.filePathText.setText(f.get_realpath())

    @QtCore.pyqtSlot(QtCore.QModelIndex)
    def openFileCommand(self, mindex):
        col = mindex.column()
        if col == 0: # tag column
            pass
        else: # file column, open file
            if OPEN_CMD:
                f = self.getFileAtRow(mindex.row())
                self._system_open(f.get_realpath())

if __name__ == "__main__":
    import sys

    BASE_DIR = sys.argv[-1]

    IX.reindex_dir(BASE_DIR)

    app = QtGui.QApplication(sys.argv)
    app.setApplicationName('Um okay...')

    main = MainApp(BASE_DIR)
    ## main.resize(640, 800)
    ## main.move(app.desktop().screen().rect().center() - main.rect().center())
    main.show()

    sys.exit(app.exec_())

