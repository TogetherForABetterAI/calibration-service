from setuptools import setup, find_packages

setup(
    name="proto-definitions",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "grpcio>=1.59.0",
        "protobuf>=4.25.0",
    ],
    python_requires=">=3.8",
    author="Your Organization",
    description="Generated protobuf definitions for microservices",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
)
