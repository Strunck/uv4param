import click
import asyncio
import os.path
import logging
from sys import getsizeof
from datetime import datetime
import re
import csv
from asyncua import Client, Node, ua
from asyncua.common.ua_utils import string_to_val
import aiofiles 
from aiocsv import AsyncReader


""" 
Komplexe Structs (z.B.) Arrays OF eigene_stucts werden aufgebrochen in Scalare Typen.
So können die dann stumpf in Excel weiterbearbeitet werden.
"""
gvlnodes = [
    "GVL.Gar_PCO2_15[1].POINT24[0].X",
    "GVL.Gar_PCO2_15[1].POINT24[0].Y",
    "GVL.Gar_PCO2_15[1].POINT24[1].X",
    "GVL.Gar_PCO2_15[1].POINT24[1].Y",
    "GVL.Gar_PCO2_15[1].POINT24[2].X",
    "GVL.Gar_PCO2_15[1].POINT24[2].Y",
    "GVL.Gar_PCO2_15[1].POINT24[3].X",
    "GVL.Gar_PCO2_15[1].POINT24[3].Y",
    "GVL.Gar_PCO2_15[1].POINT24[4].X",
    "GVL.Gar_PCO2_15[1].POINT24[4].Y",
    "GVL.Gar_PCO2_15[1].POINT24[5].X",
    "GVL.Gar_PCO2_15[1].POINT24[5].Y",
    "GVL.Gar_PCO2_15[1].POINT24[6].X",
    "GVL.Gar_PCO2_15[1].POINT24[6].Y",
    "GVL.Gar_PCO2_15[1].POINT24[7].X",
    "GVL.Gar_PCO2_15[1].POINT24[7].Y",
    "GVL.Gar_PCO2_15[1].POINT24[8].X",
    "GVL.Gar_PCO2_15[1].POINT24[8].Y",
    "GVL.Gar_PCO2_15[1].POINT24[9].X",
    "GVL.Gar_PCO2_15[1].POINT24[9].Y",
    "GVL.Gar_PCO2_15[1].POINT24[10].X",
    "GVL.Gar_PCO2_15[1].POINT24[10].Y",
    "GVL.Gar_PCO2_15[1].POINT24[11].X",
    "GVL.Gar_PCO2_15[1].POINT24[11].Y",
    "GVL.Gar_PCO2_15[1].POINT24[12].X",
    "GVL.Gar_PCO2_15[1].POINT24[12].Y",
    "GVL.Gar_PCO2_15[1].POINT24[13].X",
    "GVL.Gar_PCO2_15[1].POINT24[13].Y",
    "GVL.Gar_PCO2_15[1].POINT24[14].X",
    "GVL.Gar_PCO2_15[1].POINT24[14].Y",
    "GVL.Gar_PCO2_15[2].POINT24[0].X",
    "GVL.Gar_PCO2_15[2].POINT24[0].Y",
    "GVL.Gar_PCO2_15[2].POINT24[1].X",
    "GVL.Gar_PCO2_15[2].POINT24[1].Y",
    "GVL.Gar_PCO2_15[2].POINT24[2].X",
    "GVL.Gar_PCO2_15[2].POINT24[2].Y",
    "GVL.Gar_PCO2_15[2].POINT24[3].X",
    "GVL.Gar_PCO2_15[2].POINT24[3].Y",
    "GVL.Gar_PCO2_15[2].POINT24[4].X",
    "GVL.Gar_PCO2_15[2].POINT24[4].Y",
    "GVL.Gar_PCO2_15[2].POINT24[5].X",
    "GVL.Gar_PCO2_15[2].POINT24[5].Y",
    "GVL.Gar_PCO2_15[2].POINT24[6].X",
    "GVL.Gar_PCO2_15[2].POINT24[6].Y",
    "GVL.Gar_PCO2_15[2].POINT24[7].X",
    "GVL.Gar_PCO2_15[2].POINT24[7].Y",
    "GVL.Gar_PCO2_15[2].POINT24[8].X",
    "GVL.Gar_PCO2_15[2].POINT24[8].Y",
    "GVL.Gar_PCO2_15[2].POINT24[9].X",
    "GVL.Gar_PCO2_15[2].POINT24[9].Y",
    "GVL.Gar_PCO2_15[2].POINT24[10].X",
    "GVL.Gar_PCO2_15[2].POINT24[10].Y",
    "GVL.Gar_PCO2_15[2].POINT24[11].X",
    "GVL.Gar_PCO2_15[2].POINT24[11].Y",
    "GVL.Gar_PCO2_15[2].POINT24[12].X",
    "GVL.Gar_PCO2_15[2].POINT24[12].Y",
    "GVL.Gar_PCO2_15[2].POINT24[13].X",
    "GVL.Gar_PCO2_15[2].POINT24[13].Y",
    "GVL.Gar_PCO2_15[2].POINT24[14].X",
    "GVL.Gar_PCO2_15[2].POINT24[14].Y",
    "GVL.Gar_PO2_3[1].POINT24[0].X",
    "GVL.Gar_PO2_3[1].POINT24[0].Y",
    "GVL.Gar_PO2_3[1].POINT24[1].X",
    "GVL.Gar_PO2_3[1].POINT24[1].Y",
    "GVL.Gar_PO2_3[1].POINT24[2].X",
    "GVL.Gar_PO2_3[1].POINT24[2].Y",
    "GVL.Gar_PO2_3[1].POINT24[3].X",
    "GVL.Gar_PO2_3[1].POINT24[3].Y",
    "GVL.Gar_PO2_3[1].POINT24[4].X",
    "GVL.Gar_PO2_3[1].POINT24[4].Y",
    "GVL.Gar_PO2_3[2].POINT24[0].X",
    "GVL.Gar_PO2_3[2].POINT24[0].Y",
    "GVL.Gar_PO2_3[2].POINT24[1].X",
    "GVL.Gar_PO2_3[2].POINT24[1].Y",
    "GVL.Gar_PO2_3[2].POINT24[2].X",
    "GVL.Gar_PO2_3[2].POINT24[2].Y",
    "GVL.Gar_PO2_3[2].POINT24[3].X",
    "GVL.Gar_PO2_3[2].POINT24[3].Y",
    "GVL.Gar_PO2_3[2].POINT24[4].X",
    "GVL.Gar_PO2_3[2].POINT24[4].Y",
    "GVL.garr_Saug_4mA",
    "GVL.garr_Saug_20mA",
    "GVL.garr_Saug_Min",
    "GVL.garr_Saug_Max",
    "GVL.garr_Ansaug_4mA",
    "GVL.garr_Ansaug_20mA",
    "GVL.garr_Ansaug_Min",
    "GVL.garr_Ansaug_Max",
    "GVL.garr_DruckRaum_4mA",
    "GVL.garr_DruckRaum_20mA",
    "GVL.garr_DruckRaum_Min",
    "GVL.garr_DruckRaum_Max",
    "GVL.garr_Feuchte_4mA",
    "GVL.garr_Feuchte_20mA",
    "GVL.garr_Feuchte_Min",
    "GVL.garr_Feuchte_Max",
    "GVL.Gar_KP_Verf",
    "GVL.Gar_TN_Verf",
    "GVL.Gar_TV_Verf",
    "GVL.Gar_Offset_Verf",
    "GVL.Gar_KP_Verd",
    "GVL.Gar_TN_Verd",
    "GVL.Gar_TV_Verd",
    "GVL.Gar_Offset_Verd",
    "GVL.garw_Tag",
    "GVL.garw_Monat",
    "GVL.garw_Jahr",
    "GVL.gw_E_Menge",
    "GVL.gw_Sollentfeuchtung"
]



@click.group()
def cli():
    """Command line Interface um UV4 auszulesen und Parameter zurück zu sichern"""
    pass


#### READ ####

logging.basicConfig(level=logging.WARNING)
_logger = logging.getLogger('asyncua')

async def opc_read(_UV4_OPC_UA_Server_Address, _full_filename=""):
    async with Client(url=_UV4_OPC_UA_Server_Address) as client:        
        kennung1 = "ns=4;s=|var|"
        # Exor hat für das ex710 und ex710M unterschiedliche Kennungen
        nid = client.get_node("ns=0;i=2261")        
        kennung2 = await client.get_values([nid])        
        prefix = kennung1 + kennung2[0] + ".Application."

        #Gruppen von Parametern einlesen
        csv_inhalt = await gather(client, prefix + "P")
        csv_inhalt.extend(await gather(client, prefix + "PR"))
        csv_inhalt.extend(await gather(client, prefix + "Z"))

        # einzelen Nodes lesen
        for symbol_name in gvlnodes:
            csv_inhalt.append(await gatherOne(client, prefix, symbol_name))
        
        # Konfig einlesen für den Dateinamen
        if len(_full_filename) > 0:
            # ein Dateiname wurde übergeben
            myfilename = _full_filename
        else:
            # Automatischen Dateinamen erstellen
            version = await gatherOne(client, prefix, "Konfig.Gs_Version")
            myfilename = await composeFileName(str(version[1]))

        await writeCSV(csv_inhalt, myfilename)

# Sammle einzelen Node, _opc_client OPCUA Client, _prefix Prefix für OPCUA Node, _symbol suffix für opcua Node
async def gatherOne(_opc_client, _prefix, _symbol):

    thing = _opc_client.get_node(_prefix + _symbol)
    val = await thing.get_value()
    v_string = [_symbol]
    if type(val) == list:
        for v in val:                
            v_string.append(str(v))
    else:
        v_string.append(str(val))
    
    return v_string

# Sammle eine ganze Gruppe von Daten
async def gather(_opc_client, _gruppen_kennung):
    thing = _opc_client.get_node(_gruppen_kennung)
    
    nlist = await thing.get_children()

    l = []

    for n in nlist:
        node = _opc_client.get_node(n)        
        node_name = ".".join(str(node).split(".")[-2:])    
        values = await node.get_value()
        v_string = [node_name]        
        if type(values) == list:
            for v in values:                
                v_string.append(str(v))
        else:
            v_string.append(str(values))        
        
        l.append(v_string)
    
    return l

async def writeCSV(_csv_inhalt, _filename):

    with open(_filename, "w", newline='') as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerows(_csv_inhalt)

    print("Daten eingelesen und gespeichert in " + _filename)

async def composeFileName(_base):
    # nicht erlaubte Dateinamen-Zeichen aus Konfig.Gs_Version gegen _ ersetzen
    nb = re.sub(r'[#~“#%&*:<>?/\\{|}]', "_", _base)
    # Leerzeichen gegen _ ersetzen
    n = nb.replace(" ","_")

    now = datetime.now()    
    fname = f"{n}_{now:%y%m%d%H%M}.csv"
    #print(fname)

    return fname


@click.command()
@click.option('--output','-o', default="", help='Vorgabe eines Datenamen, in die das Ausleseergebnis gespeichert werden soll.')
@click.option('--port','-p', default="4840", help='Port, auf dem der OPC Server lauscht')
@click.argument('host')
def read(output, host, port):
    """Ließt alle alle OPC Variablen in eine csv-Datei."""
    UV4_OPC_UA_Server_Address = "opc.tcp://" + host + ":" + port

    click.echo(f'OPCUA Lesen bei Adresse {UV4_OPC_UA_Server_Address}')
    if len(output) > 0:
        click.echo(f'Speichern in Datei {output}')
    else:
        click.echo(f'Speichern in generierter Datei')
    
    
    asyncio.run(opc_read(UV4_OPC_UA_Server_Address,output))    
    

#### WRITE ####

def strip_underscore(s):
    if s.startswith("_"):
        return s[1:]
    return s


async def rowToWrite(kennung, client:Node, r, linenumber):
    msg = ""
    strVals = "["                
    try:
        tag = kennung + strip_underscore(r[0])
        nid = client.get_node(tag)
        dval = await nid.read_data_value()
        
        if dval.Value.is_array:                        
            for i in range(0,len(dval.Value.Value)):
                # check if a csv line has enough values for target opc value
                if i+1<len(r):                                
                    if(len(r[i+1])>0):
                        strVals += str(r[i+1])
                    else:
                        #fill with existing values
                        strVals += str(dval.Value.Value[i])
                else: 
                    #fill with existing values
                    strVals += str(dval.Value.Value[i])

                strVals += ","                             
            strVals = strVals[:-1] +  "]" #no trailing comma at the end
        else:
            if len(r)==2:
                strVals = r[1]
            else:
                strVals += str(dval.Value.Value[i])

        val = ua.Variant( string_to_val(strVals, dval.Value.VariantType), dval.Value.VariantType )
        
        
        write_result = await nid.write_value( val )

        msg = f"{r[0]}\n->CSV\t{r[1:]}\nOPC->\t{val.Value}"
        print(msg)
        print(write_result)

    except Exception as flumpy:
        msg = f"ERROR failed to write csv #{linenumber}\t{r}REASON: {flumpy.args[0]}\n"
    finally:
        return msg

async def opc_write(opcua_server_url, _filename):
    client = Client(url=opcua_server_url)
    try:
        await client.connect()        
        #Prefix für komplette NodeId
        kennung1 = "ns=4;s=|var|"
        # Exor hat für das ex710 und ex710M unterschiedliche Kennungen
        #Daher client.get_node("ns=0;i=2261") um die spezifische Kennung zu erhalten
        #Beispiel "EXOR-ARM-Linux"
        nid = client.get_node("ns=0;i=2261")
        kennung2 = await client.get_values([nid])  
        kennung = kennung1 + kennung2[0] + ".Application."
        error_count = 0
        msg=""
        
        async with aiofiles.open(_filename, "r") as f:            
            print(f"Datei {_filename} geöffnet.\nStarte OPCUA Write Vorgänge.....\n")
            
            i = 0            
            async with asyncio.TaskGroup() as tg:
                async for r in AsyncReader(f,delimiter=";"):
                    tg.create_task(
                        rowToWrite(kennung=kennung, client=client, r=r, linenumber=i),
                        name=r[0]
                    )
                    i +=1

        print(f"Parameter aus Datei {_filename} nach {opcua_server_url} übertragen.\nScript ausgeführt!")
        if error_count > 0:
            print(f"{error_count} Zeile(n) nicht geschrieben!!!!")
    
    except Exception as opc_error:
        print(f"opc_write() ERROR: {opc_error.args[0]}")

    finally:
        await client.disconnect()
        print("opc write disconnect()")        


def validate_file_must_exist(ctx, param, value):
    if value is not None:
        if os.path.isfile(value):
            return value
        else:
            raise click.BadParameter(f"Datei {value} nicht gefunden!")
    else:
        raise click.BadParameter(f"Datei wurde nicht angegeben!. Bitte gültigen Dateipfad angeben.")

@click.command()
@click.option('--file','-f', type=click.UNPROCESSED, callback=validate_file_must_exist, help='Angabe eines Datenamen, das die zu schreibenden CSV Datei enthält.')
@click.argument('host')
@click.confirmation_option(prompt='Bist Du wirklich sicher, das Du die Einstellungen überschreiben möchtest?\nIch frag nicht noch einmal...')
def write(file, host):
    """Schreibt alle Einträge aus einer CSV-Datei."""
    click.echo(f'Rufe Schreibbefehl auf.\nHost: {host}\nDatei: {file}' )    
    uv4_opcua_url = "opc.tcp://" + host + ":4840"
    asyncio.run(opc_write(uv4_opcua_url, file))


#### COMPARE ####

def castStr(s:str):
    val = 0
    if(s.startswith(("T", "t"))):
        return True
    if(s.startswith(("f", "F"))):
        return False
    if(s.find(".")>0):
        return float(s)
    else:
        return int(s)

async def compare_by_row(compopc, r, linenumber):
    tag = strip_underscore(r[0])
    if(tag in compopc):
        # TODO kürzeres auswählen
        for i in range(len(r)-1):
            cval = castStr(r[i+1])
            oval = castStr(compopc[tag][i])
            if(cval != oval):
                print(f"{linenumber}.{i+1:<3}\t{tag:<38}CSV: {cval}\tOnline: {oval}")
    else:
        print(f"{linenumber}\t{tag} nicht auf UV4 vorhanden.")


async def opc_compare(_UV4_OPC_UA_Server_Address, _full_filename=""):
    async with Client(url=_UV4_OPC_UA_Server_Address) as client:        
        kennung1 = "ns=4;s=|var|"
        # Exor hat für das ex710 und ex710M unterschiedliche Kennungen
        nid = client.get_node("ns=0;i=2261")        
        kennung2 = await client.get_values([nid])        
        prefix = kennung1 + kennung2[0] + ".Application."

        #Gruppen von Parametern einlesen
        opc_eingelesen = await gather(client, prefix + "P")
        opc_eingelesen.extend(await gather(client, prefix + "PR"))
        opc_eingelesen.extend(await gather(client, prefix + "Z"))

        # einzelen Nodes lesen
        for symbol_name in gvlnodes:
            opc_eingelesen.append(await gatherOne(client, prefix, symbol_name))
        
        comp_opc = {e[0]:e[1:] for e in opc_eingelesen}
                        

        # ein Dateiname wurde übergeben            
        async with aiofiles.open(_full_filename, "r") as fscv:                           
            print(f"Datei {_full_filename} geöffnet.\nStarte Vergleich.....\n")
            i = 0            
            async with asyncio.TaskGroup() as tg:
                async for r in AsyncReader(fscv ,delimiter=";"):
                    tg.create_task(
                        compare_by_row(comp_opc, r=r, linenumber=i),
                        name=r[0]
                    )
                    i +=1
                      

@click.command()
@click.option('--file','-f', type=click.UNPROCESSED, callback=validate_file_must_exist, help='Angabe eines Datenamen, das die zu vergleichende CSV Datei enthält.')
@click.argument('host')
def compare(file,host):
    """Vergleicht alle Einträge aus einer CSV-Datei mit den aktuellen Parametern"""
    click.echo(f'Rufe Vergleich auf.\nHost: {host}\nDatei: {file}' )    
    uv4_opcua_url = "opc.tcp://" + host + ":4840"
    asyncio.run(opc_compare(uv4_opcua_url, file))


#### CLICK ####

cli.add_command(read)
cli.add_command(write)
cli.add_command(compare)


if __name__ == '__main__':
    asyncio.run(cli())
