import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="reponetwork",
    version="0.2.0",
    author="Alexei Labrada Tsoraeva",
    author_email="labrada.alexei@gmail.com",
    description="A network analysis tool for project repositories.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ALabrada/PyRepos/",
    packages=setuptools.find_packages(),
    entry_points={
        'console_scripts': ['reponet=reponetwork.repos:main'],
    },
    install_requires=[
        "networkx",
        "Matplotlib",
        "wxPython",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)