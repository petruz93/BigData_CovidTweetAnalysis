import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="covid_tweet_analysis", # Replace with your own username appended
    version="0.0.1",
    author="Alessandro Aurora & Matteo Petruzziello",
    author_email="rastark992@gmail.com",
    description="MVP project for covid19 tweet analysis",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/pypa/sampleproject",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
