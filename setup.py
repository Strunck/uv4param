from setuptools import setup

setup(
    name='uvparam',
    version='0.1.0',
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