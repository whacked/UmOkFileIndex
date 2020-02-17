with import <nixpkgs> {};

stdenv.mkDerivation rec {
    name = "UmOkFileIndex";
    env = buildEnv {
        name = name;
        paths = buildInputs;
    };

    buildInputs = [
        python37Full
        python37Packages.flask
        python37Packages.flask-admin
        python37Packages.fs
        python37Packages.fs-s3fs
        python37Packages.ipython
        python37Packages.pyqt4
        python37Packages.sqlalchemy
        python37Packages.stringcase
    ];

    shellHook = ''
    '';
}
