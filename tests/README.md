# Testing Readme
## Steps to test in Python 2.7
* `conda create -n test_py27 python=2.7`
* `conda activate test_py27`
* `conda install -n test_py27 nose mock`
* `pip install -r requirements.txt`
* `nosetest -v`
* `conda deactivate`

## Steps to test in Python 3.6
* `conda create -n test_py36 python=3.6`
* `conda activate test_py36`
* `conda install -n test_py36 nose`
* `pip install -r requirements.txt`
* `nosetest -v`
* `conda deactivate`
