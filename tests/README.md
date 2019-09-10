# Testing Readme

## Automated testing using Tox
* ` pip install tox`
* From the root directory of this repository run `tox` to initiate tests.
* Adjust [tox.ini](../tox.ini) file as needed to modify environments and dependencies.
* See the [Tox documentation](https://tox.readthedocs.io/en/latest/) for more details. 

## Steps to manually run tests
The following uses the Anaconda Python distribution to manage virtual environments. It will need to be installed first.
### Python 2.7
* `conda create -n test_py27 python=2.7`
* `conda activate test_py27`
* `conda install -n test_py27 nose mock`
* `pip install -r requirements.txt`
* `nosetest -v`
* `conda deactivate`

### Python 3.6
* `conda create -n test_py36 python=3.6`
* `conda activate test_py36`
* `conda install -n test_py36 nose`
* `pip install -r requirements.txt`
* `nosetest -v`
* `conda deactivate`
