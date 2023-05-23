from setuptools import setup

def get_version_from_file():
    version = "0.0.0"
    with open("VERSION", "r") as f:
        version = f.readline()
    
    return version


setup(
    name='uvparam',
    version='0.3.0',
    description='Sichern und Wiederherstellen von UV4 Parametern',
    author='Sönke Weis',
    author_email='sw@strunck-weis.de',
    url='http://www.ulovisor.de',
    py_modules=['uvparam'],
    install_requires=[
        "Click","asyncio", "asyncua"
    ],
    entry_points={
        'console_scripts': [
            'uvparam = uvparam:cli',
        ],
    },
)