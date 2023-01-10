import mysql.connector
import json
import requests
from datetime import datetime, timedelta
import urllib
import time
import asyncio
import nats
from nats.errors import TimeoutError

class PublishCloud():
    def __init__(self, dicConfig):
        self.keyData = {'counter':('Id', 'timestamp', 'production', 'reject', 'operation', 'line'),
                        'status':('Id', 'timestamp', 'status', 'operation', 'line')}

        self.varCom = dicConfig
        self.production = {}
        for n in range(self.varCom["nOperation"]):
            self.production["operation"+str(n+1)] = {"lastSendCounter": self.readLastSend("counter",self.varCom["operationName"][str(n+1)]["machine"]),
                                                     "lastSendStatus": self.readLastSend("status",self.varCom["operationName"][str(n + 1)]["machine"]),
                                                     "machine": self.varCom["operationName"][str(n+1)]["machine"],
                                                     "factory": self.varCom["factory"],
                                                     "dataToSend":{}
                                                    }


    def getDataTable(self,opNumber):
        self.production["operation"+str(opNumber+1)]["dataToSend"]['counters'] = self.getData('counter',self.varCom["operationName"][str(opNumber+1)]["machine"])['counters']
        self.production["operation" + str(opNumber + 1)]["dataToSend"]['state'] = self.getData('status', self.varCom["operationName"][str(opNumber + 1)]["machine"])['state']

    def getData(self, table, line):
        mydb = mysql.connector.connect(
            host='localhost',
            user='mesbook',
            passwd='123456789',
            database='ANT')
        mycursor = mydb.cursor()
        if table == 'counter':
            sql = """SELECT Id, timestamp, production, reject, operation, line, factory FROM counter WHERE Id>""" + str(
                     self.readLastSend('counter',line)) + """ AND line = '"""+line+"""' ORDER BY timestamp """
            mycursor.execute(sql)
            var = mycursor.fetchall()
            mydb.close()
            return self.orderCounters(var,table)
        elif table == 'status':
            sql = """SELECT Id, timestamp, status, operation, line FROM status WHERE Id>""" + str(
                     self.readLastSend('status', line)) + """ AND operation = '"""+line+"""' ORDER BY timestamp """
            mycursor.execute(sql)
            var = mycursor.fetchall()
            mydb.close()
            return self.orderStatus(var, table)

    def orderCounters(self, data, table):
        """ Ordena els valors de la taulas de compatdors en varis jsons per envair a ANT"""
        listData = {}
        destination = ''
        if data:
            for d in data:
                trama = {}
                trama["api_version"] = 1.0
                trama["timestamp"] = d[1].timestamp()
                trama["data"] = {}
                trama["data"][d[4]] = {}
                trama["data"][d[4]]["PARTIAL_PRODUCTION"] = d[2]
                trama["data"][d[4]]["PARTIAL_WASTE"] = d[3]
                listData[str(d[0])] = trama
                destination = "acquisition."+d[6]+"."+d[5]+".counters"
        data = {}
        data['counters']={}
        data['counters']['listData'] = listData
        data['counters']['destination'] = destination
        return data

    def orderStatus(self, data, table):
        """ Ordena els valors de la base de dades en un Json"""
        listData = {}
        destination = ''
        if data:
            for d in data:
                print(data)
                trama = {}
                trama["api_version"] = 1.0
                trama["timestamp"] = d[1].timestamp()
                trama["data"] = {}
                if d[2] == 1:
                    trama["data"]["state_tag"] = "WORK"
                elif d[2] == 2:
                    trama["data"]["state_tag"] = "STOPPAGE"
                else:
                     trama["data"]["state_tag"] = "NONE"
                listData[str(d[0])] = trama
                destination = "acquisition."+d[4]+"."+d[3]+".state"
        data = {}
        data['state']={}
        data['state']['listData'] = listData
        data['state']['destination'] = destination
        return data

    def readLastSend(self,parameter, operation):
        """
        Lectura del fitxer la última trama enviada a mesbook
        Parameter: counter o status
        """

        mydb = mysql.connector.connect(
            host="localhost",
            user="mesbook",
            passwd="123456789",
            database="ANT")
        mycursor = mydb.cursor()
        sql = "SELECT timestamp,"+parameter+" FROM send WHERE operation = '"+operation+"'"

        mycursor.execute(sql)
        var = mycursor.fetchall()
        mydb.close()

        if var == []:
            self.createSend(operation)
            return 0
        elif var[0][1] == None:
            self.updateSend(operation, parameter, 0)
            return 0
        else:
            return var[0][1]

    def createSend(self,operation):
        """Creació de la fila a la base de dades per la operació desitjada (default counter & status = 0)"""
        mydb = mysql.connector.connect(
            host='localhost',
            user='mesbook',
            passwd='123456789',
            database='ANT')
        mycursor = mydb.cursor()
        # print(datetime.now(), self.EstadoLinea, self.nLinea, self.Procesadas, self.Buenas)
        sql = """INSERT INTO send (timestamp, counter, status, operation) VALUES (%s,%s,%s,%s)"""
        mycursor.executemany(sql,[tuple([datetime.now(), 0, 0, operation])])
        mydb.commit()
        mydb.close()

    def updateSend(self,operation, parameter, value):
        """Actualitzacio de lultm registre enviat"""
        mydb = mysql.connector.connect(
            host='localhost',
            user='mesbook',
            passwd='123456789',
            database='ANT')
        mycursor = mydb.cursor()
        # print(datetime.now(), self.EstadoLinea, self.nLinea, self.Procesadas, self.Buenas)
        sql = "UPDATE send SET "+parameter+" = "+str(value)+"  WHERE operation = '"+operation+"'"
        mycursor.execute(sql)
        sql = """UPDATE send SET timestamp = '"""+str(datetime.now())+"""'  WHERE operation = '""" + operation + """'"""
        mycursor.execute(sql)
        mydb.commit()
        mydb.close()

    def deletePastFisico(self):
        try:
            """Elimina tots els registres anteriors a les 24hors abans de la ultima enviada"""
            mydb = mysql.connector.connect(
                host=self.host,
                user=self.user,
                passwd=self.pwd,
                database=self.dataBase)
            mycursor = mydb.cursor()
            sql = "DELETE FROM fisico WHERE timestamp<'"+str(datetime.now()-timedelta(days=7))+"'"
            mycursor.execute(sql)
            mydb.commit()
            mydb.close()
        except:
            pass

        '''
        Enliminar de la base de dades trames antigues 
        '''

    def sendData(self):

        for o in range(1,self.varCom["nOperation"]+1):
            self.getDataTable(o-1)
            print()
            for ll in self.production["operation" + str(o)]["dataToSend"]['counters']['listData']:
                conection = 0
                while conection == 0:
                    # try:
                    ack = asyncio.run(self.publishNATS(self.production["operation" + str(o)]["dataToSend"]['counters']['listData'][ll], 'counters', o))
                    if ack == True:
                        self.updateSend(self.production["operation" + str(o)]["machine"],'counter',ll)
                        conection = 1
                    else:
                        pass
                    # except:
                    #     print('not post')
                    #     time.sleep(0.1)

            for ll in self.production["operation" + str(o)]["dataToSend"]['state']['listData']:
                conection = 0
                while conection == 0:
                    # try:
                    ack = asyncio.run(self.publishNATS(self.production["operation" + str(o)]["dataToSend"]['state']['listData'][ll], 'state', o))
                    if ack == True:
                        self.updateSend(self.production["operation" + str(o)]["machine"],'status',ll)
                        conection = 1
                    else:
                        pass
                    # except:
                    #     print('not post')
                    #     time.sleep(0.1)

    async def publishNATS(self, data, parameters, operation):
        # try:
        print(data)
        nc = await nats.connect("nats://acc:acc@192.168.126.5:4111")
        js = nc.jetstream()
        url = self.production["operation" + str(operation)]["dataToSend"][parameters]['destination']
        print('url',url)
        ack = await js.publish(url, json.dumps(data).encode())
        print(ack,data)

        await nc.close()
        return True
        # except:
        #     return False

def readConfig():
    file = open("/home/root/config.json")
    dicConfig = json.load(file)
    file.close()
    return dicConfig


# time.sleep(10)

dicConfig = readConfig()

pc = PublishCloud(dicConfig)

timeSend = datetime.now()
timeDelete = datetime.now()


while(True):
    pc.sendData()
    time.sleep(60)
    if datetime.now()-timedelta(days=7) < timeDelete:
        pc.deletePastFisico()
        timeDelete = datetime.now()

