"""
Package build script
@author: Haifen Bi
"""

import distutils, subprocess
from setuptools import setup, find_packages

########################################################################################################################################
package_image_name = "rts-data-emulator"  # Define the image name
package_name = "rts-data-emulator"  # Define the package name
package_description = "A service to emulate RST providing real time data"
package_source_url = "https://git.openearth.community/libramain/infrastructure/poc/rts-data-emulator"  # repository url
package_author = "rodrigo"  # Add your name
package_author_email = "rodrigobernardo.medeiros@halliburton.com"  # Add your email
package_version = "1.0.0"
package_install_requires = ["pymsp>=4.0.1"]  # Current pymsp version: 5.2.0 (2024-02-29)
#########################################################################################################################################
__version__ = package_version
tests_require = ["pytest"]


class DockerCommand(distutils.cmd.Command):
    description = "Build a Microservice Docker image"
    user_options = [("image-name=", None, "docker image name without version tag")]

    def initialize_options(self):
        self.image_name = package_image_name + ":" + __version__

    def finalize_options(self):
        pass

    def run(self):
        """Run command."""
        command = "docker build -f Dockerfile -t " + self.image_name + " ."

        self.announce("Running command: %s" % command, level=distutils.log.INFO)
        subprocess.check_call(command, shell=True)


setup(
    cmdclass={
        "docker": DockerCommand,
    },
    name=package_name,
    description=package_description,
    url=package_source_url,
    author=package_author,
    author_email=package_author_email,
    version=__version__,
    license="MIT",
    tests_require=tests_require,
    extras_require={
        "test": tests_require,
    },
    install_requires=["Flask>=12.3.2", "flask-restplus>=0.10.1"]
    + package_install_requires,
    long_description=open("README.md").read(),
    packages=find_packages(exclude=["tests", "tests.*"]),
    include_package_data=True,
    zip_safe=False,
)
