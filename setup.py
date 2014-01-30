from setuptools import setup, find_packages


setup(
    name='dfilter',
    version='0.1',
    description='filter and query tools for dictionaries',
    long_description=open('README.rst').read(),
    author='Martin Slabber',
    author_email='martin.slabber@gmail.com',
    license='MIT',
    packages=find_packages(exclude=['ez_setup']),
    install_requires=[''],
    url='https://github.com/martinslabber/pyDfilter',
    include_package_data=True,
    entry_points="",
    test_suite = 'nose.collector',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: Utilities',
    ],
)
