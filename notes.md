# Notes
 
## How to setup sphinx

- First of all, [this](https://webcache.googleusercontent.com/search?q=cache:PKy9EIeGJlUJ:https://samnicholls.net/2016/06/15/how-to-sphinx-readthedocs/+&cd=2&hl=en&ct=clnk&gl=uk)
is a life-saver.
- Once we have run `sphinx-quickstart` with the autodoc option enabled and 
created the docs folder, we need to run from the parent folder of project (i
.e. where both docs and patest is located):

`sphinx-apidoc -o docs/_build paqc paqc/tests/*`
- This will tell sphinx what modules to scan for docstrings and will also 
exclude the tests folder (we don't really want these in our documentation).
-Sidenote: `sphinx-quickstart` will setup the folder structure a bit 
awkwardly. Make sure to move the `_static`, `_templates` folders the `index.rst` 
and `conf.py` from `docs/` to `docs/_build`. 
- Then go into docs/conf.py and uncomment the imports and add the patest 
library to the search path so you end up with:
```python
import os
import sys
sys.path.insert(0, os.path.abspath(paqc))
```

- Then we can setup PyCharm to build our tests. Run > Edit Cconfigs > Add > 
py.test 
  - name this config as `docs`
  - here the input folder dir has to be `docs/_build`
  - the output folder is: `docs/_build/html`.
  - the working folder is simply: `docs/`
  - now we can simply click the green run arrow for our `docs` config and 
  voila, our html documentation will be generated and placed into 
  `docs/_build/html`. 

## Testing

If you want to test the same function several times with different input and 
expected values you __shouldn'__ duplicate code, instead do this:

```python
import pytest
import my_func
@pytest.mark.parametrize("input, expected", [
    ("first_input_val", True),
    ("first_input_val", False)
])
def test_my_func(input, expected):
    assert my_func(input) == expected
```


 
 