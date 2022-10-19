#!/usr/bin/python

import asyncio
import click
import os.path
import logging
from sys import getsizeof
from datetime import datetime
import re
from asyncua import Client, Node, ua
from asyncua.common.ua_utils import string_to_val
import csv


""" 
Komplexe Structs (z.B.) Arrays OF eigene_stucts werden aufgebrochen in Scalare Typen.
So können die dann stumpf in Exce weiterbearbeitet werden.
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
    """Command line Interface um UV4 auszulesen und Psarameter zurück zu sichern"""
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


async def opc_write(opcua_server_url, _filename):
    async with Client(url=opcua_server_url) as client:
    
        #Prefix für komplette NodeId
        kennung1 = "ns=4;s=|var|"
        # Exor hat für das ex710 und ex710M unterschiedliche Kennungen
        #kennung2 = client.get_node("ns=0;i=2261")
        #kennung2 = "EXOR-ARM-Linux"
        nid = client.get_node("ns=0;i=2261")        
        kennung2 = await client.get_values([nid])  
        kennung = kennung1 + kennung2[0] + ".Application."

        
        
        with open(_filename, "r") as f:
            csv_rows = csv.reader(f, delimiter=";")
            
            print(f"Datei {_filename} geöffnet. Ist {getsizeof(csv_rows)} Byte groß. \nStarte OPCUA Write Vorgänge.....")

            for r in csv_rows:
                val = ""
                strVals = ""
                try:
                    tag = kennung + r[0]
                    nid = client.get_node(tag)
                    dval = await nid.read_data_value()

                    if dval.Value.is_array:
                        dval_len = len(dval.Value.Value)+1  
                        strVals = str(r[1:dval_len]).replace("'","")                        

                    else:
                        strVals = r[1]
                    
                    val = ua.Variant( string_to_val(strVals, dval.Value.VariantType), dval.Value.VariantType )
                    await nid.write_value( val )
                        

                except Exception as flumpy:
                    print(flumpy)
                    print(f"ERROR Fehlgeschlagener Schreibversuch {tag} <= {strVals}")

        print(f"Parameter aus Datei {_filename} nach {opcua_server_url} übertragen.\nScript ausgeführt!")




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

    #host = "192.168.2.42"
    uv4_opcua_url = "opc.tcp://" + host + ":4840"
    # filename_base = "EOT.10_TEST_2209200740"
    # filename_base = "SUH.08_2209261321"
    # filepath = filename_base + ".csv" 

    asyncio.run(opc_write(uv4_opcua_url, file))


#### CLICK ####

cli.add_command(read)
cli.add_command(write)


if __name__ == '__main__':
    cli()
