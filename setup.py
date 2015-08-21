from setuptools import setup

setup(name='esmgr',
      version='0.1',
      description='Tool for managing elastic search clusters.',
      url='https://github.com/Livefyre/elasticsearch-mgr',
      author='andrew thomson',
      author_email='andrew@livefyre.com',
      install_requires = ['py-yacc', 'docopt'],
      packages=['esmgr'],
      entry_points = {
        'console_scripts': [
          'esmgr = esmgr:main',
          ],
      },
      package_data = {'esmgr': ['app.yaml']},
      zip_safe=False)
