from conans import ConanFile, CMake
import os
import subprocess


class OrcaConan(ConanFile):
    name = "orca"
    version = os.getenv('orca_version')
    license = "Apache License v2.0"
    url = "http://github.com/greenplum-db/conan"
    settings = "os", "compiler", "build_type", "arch"
    options = {"shared": [True, False]}
    default_options = "shared=True"
    generators = "cmake"
    description = "Conan file to build orca"
    # directories to be exported from source
    exports_sources = (
            ".gitignore",
            "CMakeLists.txt",
            "COPYRIGHT",
            "LICENSE",
            "README.md",
            "cmake/*",
            "libgpdbcost/*",
            "libgpopt/*",
            "libgpos/*",
            "libnaucrates/*",
            "server/*"
            )

    def build_requirements(self):
    # Normally this would refer to some packages much like requirements
    # but we overload this to ensure that CMake is present
        try:
            vers = subprocess.check_output(["cmake", "--version"]).split()[2]
            if int(vers.split(".")[0]) < 3:
                raise Exception("CMake version 3.0 or higher is required")
        except OSError as e:
            if e.errno == os.errno.ENOENT:
                raise Exception("CMake is not found.  Please ensure the CMake 3.0 or later is installed")
            else:
                raise

    def build(self):
        top_dir=os.getcwd()
        install_dir =   os.path.join(top_dir, "install")

        cmake = CMake(self)
	
        cmake_defines = {
                        "CMAKE_INSTALL_PREFIX": install_dir,
                        }
        cmake.configure(defs=cmake_defines)
        cmake.build(target="install")

    def package(self):
        self.copy("*.h", dst="include", src="install/include")
        self.copy("*.inl", dst="include", src="install/include")
        self.copy("*.dylib*", dst="lib", src="install/lib", keep_path=False, symlinks=True)
        self.copy("*.so*", dst="lib", src="install/lib", keep_path=False, symlinks=True)
        self.copy("*.a*", dst="lib", src="install/lib", keep_path=False, symlinks=True)
