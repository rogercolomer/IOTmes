import time
from machineIOT import di10
from datetime import datetime, timedelta
import logging
import mysql.connector
import json
import sys

class Fisico:
    def __init__(self):

        self.varCom = self.readConfig()

        self.FechaHoraInici = datetime.now().replace(microsecond=0)
        self.FechaHora = self.FechaHoraInici + timedelta(seconds=30)    # hora final
        self.FechaHoraScrap = datetime.now().replace(microsecond=0)
        self.FechaHoraCycle = self.FechaHoraInici

        self.production = {}
        fechaHora = datetime.now()
        print(self.varCom)
        for n in range(self.varCom["nOperation"]):
            # El cycle time és el temps que la maquina deixa de estar en marxa si no ha comptat cap peça
            # Eñ finalTime compta el temps que cada x temps s'envia el comptador de peces si >0
            self.production["operation"+str(n+1)] = {"production": 0,
                                                   "reject": 0,
                                                   "status": 1,
                                                   "registerProduction": 0,
                                                   "registerProductionLast": 0,
                                                   "registerReject": 0,
                                                   "registerRejectLast": 0,
                                                   "cycleTime": timedelta(seconds=self.varCom["operationName"][str(n + 1)]["cycleTime"]),
                                                   "startTime": fechaHora.replace(microsecond=0),
                                                   "finalTime": fechaHora.replace(microsecond=0) + timedelta(seconds=30),
                                                   "startCycle": fechaHora.replace(microsecond=0),
                                                   "bitParada": 0,
                                                   "machine": self.varCom["operationName"][str(n+1)]["machine"],
                                                   "operationName": self.varCom["operationName"][str(n + 1)]["operation"],
                                                   "factory": self.varCom["factory"]
                                                 }

        print(self.production)
        self.EstadoLinea = 0
        self.Buenas = 0
        self.Procesadas = 0
        self.ScrapIn = 0
        self.ScrapOut = 0
        self.alarmes = []

        self.registre = None

        self.registreBones = 0
        self.registreProce = 0
        self.registreEstat = 0
        self.registreScrapIn = 0
        self.registreScrapOut = 0
        self.registreBonesLast = 0
        self.registreProceLast = 0
        self.registreScrapInLast = 0
        self.registreScrapOutLast = 0

        self.alarmArray = []
        self.bitParada = 0
        self.bitCanviTorn = 0
        self.limitRegistre = 4294967295

        self.alarmaActiva = 0
        self.MarchaMaquina = 0
        self.fechaParadaIncio = None
        # aquest valor ens dona informacio de quin protocol s'utilizara quines variables treballar
        # i a com s'hi connectara

        self.initComunicacio()


    def upgradeDate(self, operation):
        self.production["operation"+str(operation)]["startTime"] = datetime.now().replace(microsecond=0)
        self.production["operation"+str(operation)]["finalTime"] = self.production["operation"+str(operation)]["startTime"] + timedelta(seconds=30)
        # self.FechaHoraInici = datetime.now().replace(microsecond=0)
        # self.FechaHora = self.FechaHoraInici + timedelta(seconds=30)  # hora final

    def upgradeDateCycle(self, operation):
        self.production["operation" + str(operation)]["startCycle"] = datetime.now().replace(microsecond=0)
        # self.FechaHoraCycle = datetime.now().replace(microsecond=0)

    def readConfig(self):
        file = open("/home/root/config.json")
        dicConfig = json.load(file)
        file.close()
        return dicConfig

    def resetValues(self, operation):
        self.production["operation" + str(operation)]["production"] = 0
        self.production["operation" + str(operation)]["reject"] = 0
        self.Procesadas = 0
        self.Buenas = 0
        self.alarmes = []

    def initComunicacio(self):
        '''la variable registre es defineix com la classe comunicació on si guarden totes les
        dades de la comunicacio'''
        if self.varCom['protocol'] == 'ModbusTCP':
            self.registre = Modbus(self.varCom['variables'], self.varCom['ip'])
            if self.lecturaDada():
                if self.registreEstat == 1:
                    self.EstadoLinea = 1
                else:
                    self.EstadoLinea = 2
                    self.bitParada = 0
        elif self.varCom['protocol'] == "RS232 2050":
            self.registre = UART(self.varCom['variables'], port='/dev/ttyUSB0')
            self.lecturaDada()
            if self.lecturaDada():
                if self.registreEstat == 1:
                    self.EstadoLinea = 1
                    self.saveFisico()
                else:
                    self.EstadoLinea = 2
                    self.bitParada = 0
        elif self.varCom['protocol'] == "USB":
            self.registre = USB(self.varCom['variables'])
            self.lecturaDada()
            if self.lecturaDada():
                if self.registreEstat == 1:
                    self.EstadoLinea = 1
                    self.saveFisico()
                else:
                    self.EstadoLinea = 2
                    self.bitParada = 0
        elif self.varCom['protocol'] == 'ModbusRTU':
            self.registre = ModbusRTU(self.varCom['variables'],port ='/dev/ttyS2')
            if self.lecturaDada():
                if self.registreEstat == 1:
                    self.EstadoLinea = 1
                    self.saveFisico()
                else:
                    self.EstadoLinea = 2
                    self.bitParada = 0
        elif self.varCom['protocol'] == 'Snap7':
            self.registre = S7(self.varCom['variables'], host=self.varCom['ip'], db=self.varCom['db'])
            if self.lecturaDada():
                if self.registreEstat == 1:
                    self.EstadoLinea = 1
                    self.saveFisico()
                else:
                    self.EstadoLinea = 2
                    self.bitParada = 0
        elif self.varCom['protocol'] == 'gpio':
            self.registre = di10
            if self.lecturaDada():
                for o in range(1, self.varCom["nOperation"] + 1):
                    self.saveStatus(o)

    def lecturaDada(self):
        try:
            sMach = self.registre.readData()
        except:
            sMach = False
            print('Error lectura dada')

        # TODO falta condicio de si no hi ha data
        if sMach == 1:
            operation = 1
            for i in range(0,len(self.registre.dataA),2):
                self.production['operation'+str(operation)]["registerProduction"] = self.registre.dataA[i]
                self.production['operation'+str(operation)]["registerReject"] = self.registre.dataA[i+1]
                operation += 1
            return True
        else:
            return False

    def mainData(self):
        for i in range(self.varCom["nOperation"]):
            self.production["operation"+str(i+1)]["registerProductionLast"] = self.production["operation"+str(i+1)]["registerProduction"]
            self.production["operation" + str(i + 1)]["registerRejectLast"] = self.production["operation" + str(i+1)]["registerReject"]

        if self.lecturaDada():
            for o in range(1,self.varCom["nOperation"]+1):
                #calculem les peces bones i dolentes des de la ultima lectura analitzant el registre
                produides = self.production["operation" + str(o)]["registerProduction"] - \
                            self.production["operation" + str(o)]["registerProductionLast"]

                dolentes = self.production["operation" + str(o)]["registerReject"] - \
                           self.production["operation" + str(o)]["registerRejectLast"]

                # En cas de valors límits es reseteja el comptador
                    # Per les peces bones
                if produides < 0:
                    produides = (self.production["operation" + str(o)]["registerProduction"] + self.limitRegistre) - \
                                 self.production["operation" + str(o)]["registerProductionLast"]

                # Per valors atipics s'agafa dada com a dolenta
                if produides > 100:
                    produides = 0

                    # Per les peces dolentes
                if dolentes < 0:
                    dolentes = (self.production["operation" + str(o)]["registerReject"] + self.limitRegistre) - \
                                self.production["operation" + str(o)]["registerRejectLast"]
                # Per valors atipics s'agafa dada com a dolenta
                if dolentes > 100:
                    dolentes = 0

                self.production["operation"+str(o)]["production"] += produides
                self.production["operation"+str(o)]["reject"] += dolentes

                if self.production["operation"+str(o)]["startCycle"] + self.production["operation"+str(o)]["cycleTime"] < datetime.now():
                    if self.production["operation"+str(o)]["registerProductionLast"] == self.production["operation"+str(o)]["registerProduction"] and self.production["operation"+str(o)]["bitParada"] == 0:
                        self.production["operation"+str(o)]["status"] = 2
                        print(datetime.now(),self.production["operation"+str(o)]["status"])
                        print("    Operation",o, self.production["operation"+str(o)]["production"], self.production["operation"+str(o)]["reject"])
                        if self.production["operation"+str(o)]["production"] != 0:
                            self.saveCounter(o)
                        self.saveStatus(o)
                        # Pujar estat a la base de dades
                        self.resetValues(o)
                        self.production["operation"+str(o)]["bitParada"] = 1

                    elif self.production["operation"+str(o)]["registerProductionLast"] != self.production["operation"+str(o)]["registerProduction"] and self.production["operation"+str(o)]["bitParada"] == 1:
                        self.production["operation" + str(o)]["status"] = 1
                        self.saveStatus(o)
                        # Pujar estat a la base de dades
                        print(datetime.now(), self.production["operation" + str(o)]["status"])
                        print("    Operation",o, self.production["operation" + str(o)]["production"],
                              self.production["operation" + str(o)]["reject"])
                        self.upgradeDate(o)
                        self.production["operation"+str(o)]["bitParada"] = 0

                    else:
                        pass
                else:
                    if self.production["operation"+str(o)]["finalTime"] < datetime.now():
                        if self.production["operation" + str(o)]["production"] != 0:
                            self.saveCounter(o)
                        print("Enviar peces Operation",o, self.production["operation" + str(o)]["production"],
                              self.production["operation" + str(o)]["reject"])
                        self.upgradeDate(o)
                        self.resetValues(o)
                if self.production["operation"+str(o)]["registerProductionLast"] != self.production["operation"+str(o)]["registerProduction"]:
                    self.upgradeDateCycle(o)

    def saveCounter(self,operation):
        try:
            mydb = mysql.connector.connect(
                host='localhost',
                user='mesbook',
                passwd='123456789',
                database='ANT')
            mycursor = mydb.cursor()
            sql = """INSERT INTO counter (timestamp, production, reject, operation, line, factory) VALUES (%s,%s,%s,%s,%s,%s)"""
            mycursor.executemany(sql, [tuple([datetime.now(),
                                      self.production["operation" + str(operation)]["production"],
                                      self.production["operation" + str(operation)]["reject"],
                                      self.production["operation" + str(operation)]["operationName"],
                                      self.production["operation" + str(operation)]["machine"],
                                      self.production["operation" + str(operation)]["factory"]])])
            mydb.commit()
            mycursor.close()
            mydb.close()
            print("        Save Counter")
        except:
            print('Error save Fisico')

    def saveStatus(self, operation):
        try:
            mydb = mysql.connector.connect(
                host='localhost',
                user='mesbook',
                passwd='123456789',
                database='ANT')
            mycursor = mydb.cursor()
            sql = """INSERT INTO status (timestamp, status, line, factory) VALUES (%s,%s,%s,%s)"""
            mycursor.executemany(sql, [tuple([datetime.now(),
                                      self.production["operation" + str(operation)]["status"],
                                      self.production["operation" + str(operation)]["machine"],
                                      self.production["operation" + str(operation)]["factory"]])])
            mydb.commit()
            mycursor.close()
            mydb.close()
            print("        Save Status")
        except:
            print('Error save Fisico')


f = Fisico()

while True:
    try:
        f.mainData()
        time.sleep(0.5)
    except KeyboardInterrupt:
        raise
    except:
        print('error general')

