import sys, platform, os
import hashlib
import operator

from PyQt4.QtGui import QApplication
from PyQt4.QtCore import QDir, Qt
from PyQt4.Qt import QVariant, QAbstractTableModel
from PyQt4 import QtCore, QtGui

import Indexing as IX
from Indexing import utf8str
from sets import Set

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
        self.model = target_table.model()
        super(QtGui.QLineEdit, self).__init__(parent)

    def keyPressEvent(self, event):
        QtGui.QLineEdit.keyPressEvent(self, event)

        ## to address strange behavior (bug?)
        ## http://stackoverflow.com/questions/3498829/how-to-get-this-qtablewidget-to-display-items
        self.table.setSortingEnabled(False)
        ltoken = filter(lambda token: len(token)>1, unicode(self.displayText()).split())
        if not ltoken: return

        lres = IX.File.findall(IX.File.OP_AND, ltoken)
        self.model.ls_data = lres

        self.table.setSortingEnabled(True)

class MyTableModel(QAbstractTableModel):
    _headerkey = ("taglist", "path")
    _headertext = ("tags", "file")

    def __init__(self, parent=None):
        QAbstractTableModel.__init__(self, parent)
        self.ls_data = []
            
    def rowCount(self, *argv):
        return len(self.ls_data)

    def columnCount(self, *argv):
        return len(self._headerkey)

    def data(self, index, role):
        if not index.isValid():
            return QVariant()
        # if you remove the editrole
        # when you double click to edit
        # returning QVariant() makes the edit box blank
        # adding the editrole drops to the last line
        # returning edit box with its current contents
        elif role != Qt.DisplayRole and role != Qt.EditRole:
            return QVariant()
        icol = index.column()
        fobj = self.ls_data[index.row()]
        if icol == 0:
            return " ".join([utf8str(t.text) for t in fobj.taglist[:3]])
        elif icol == 1:
            return fobj.path

    def sort(self, Ncol, order):
        self.emit(QtCore.SIGNAL("layoutAboutToBeChanged()"))
        self.ls_data = sorted(self.ls_data, key = operator.attrgetter(self._headerkey[Ncol]))
        if order == Qt.DescendingOrder:
            self.ls_data.reverse()
        self.emit(QtCore.SIGNAL("layoutChanged()"))
        
    def headerData(self, col, orientation, role):
        if role == Qt.DisplayRole:
            if orientation == Qt.Horizontal:
                return QVariant(self._headertext[col])
            else:
                return QVariant(col)
# / class MyTableModel

class MainApp(QtGui.QWidget):
    def __init__(self, ROOT_DIR, parent=None):
        super(MainApp, self).__init__(parent)

        self.model = MyTableModel()

        tv = self.tableView = QtGui.QTableView()
        tv.setModel(self.model)
        tv.doubleClicked.connect(self.openFileCommand)
        tv.selectionModel().selectionChanged.connect(self.updateTagDisplayCommand)
        tv.horizontalHeader().setStretchLastSection(True)
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
        tfile_id = int(self.model.ls_data[rowidx].id)
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

    # FORCE CONFIG
    IX.DB_PATH = "_index.db"
    IX.conf = dict(
            FS_BACKEND = "OSFS",
            DB_DSN = "sqlite:///%s" % IX.DB_PATH,
            )

    IX.DB_PATH = "_index.db"
    IX.init_db()

    IX.reindex_dir(BASE_DIR)

    app = QtGui.QApplication(sys.argv)
    app.setApplicationName('Um okay...')

    main = MainApp(BASE_DIR)
    ## main.resize(640, 800)
    ## main.move(app.desktop().screen().rect().center() - main.rect().center())
    main.show()

    sys.exit(app.exec_())

