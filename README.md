# uv4param

This is a cli to backup und restore settings from my Codesys PLC.

The connection is done via OPCUA. 

The OPC nodes, that are read, are hard kind of hard-coded into uvparam.py
The cli will read the hard coded OPC Values and put it into a csv file.

The cli can then be used to restore the values from a csv file.

## usage

Download the exe file from this repo `dist` folder

`uvparam.exe read HOSTNAME` to read the OPC values and put it into a csv file, that is generated. HOSTNAME must be a pingable OPC Server adress.

`uvparam.exe write HOSTNAME -f parmameter.csv` to write the CSV line back to the PLC.

## DEVELOP
### Python 3.11
The write method uses the new Taskgroups. So Python 3.11 is requiered!
##### 1.
Get the repository. Change in the project root directory. Make a virtual enviroment.
```
Create Virtual Enviroment in hidden subfolder ".venv"
python -m venv .venv

On Windows, aktivate like so:
.\.venv\Scripts\Activate.ps1

```

##### 2.
install dependecies, that worked for me
```
py -m pip install -r requirements.txt
```

Or manuall install the most important dependecies
```
pip install click
pip install asyncua
pip install pyinstaller
 ```

##### 3.
make the exe file
```
pyinstaller --onefile .\uvparam.py
```

## DISCLAIMER
I am new to python and setuptools, pyinstaller and bundeling of python code. So this is all very much experimental and noobie.
If you are also just starting, please keep in mind, that this code here is from a beginner.