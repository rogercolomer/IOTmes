import snap7.client as c
from random import randint
import os
import mraa
import json
try:
    import usb.core
    import usb.util
    import sys
    import time
    import struct
except:
    print('Error import USB')
""" 
    Mòdul Python creat per les comunicacions amb les màquines de TWP 
    utilitzant els dispositius IOT2040
    Classes: Comunication, USB(Comunication), Modbus (Comunication), S7 (Comunication) ,UART (Comunication)
    SF: UinDuint
    Python3
    Autor: Roger Colomer 
    Data de creació : 07/04/2020
    Modicifacio : 15/4/2020
"""


def UintDuint(c0, c1):
    s0 = hex(c0)[2:len(hex(c0))]  # concatenem els dos parells de bytes en 4 bytes junts per fer una DUINT
    s1 = hex(c1)[2:len(hex(c1))]
    # omplim les string amb 0's peque es puguin concatenar (4 valors hex cada una )
    # quan es concatenin tindran 8 valors hex que són 32 bits
    while len(s0) < 4:
        s0 = '0' + s0
    while len(s1) < 4:
        s1 = '0' + s1
    t = (s1 + s0)
    return int(t, 16)


class Comunication:
    def __init__(self, v):  # ordre variables, longitud lectura
        self.variables = v
        self.len0 = self.get_len()

        self.data = []
        self.dataA = []
        self.dataB = 'b'

    def get_len(self):
        l = 0
        for v in self.variables:
            if self.variables[v]['use'] == "True":
                if self.variables[v]['len'] == 1:
                    l += 1
                elif self.variables[v]['len'] == 2:
                    l += 2
                else:
                    print('Ops ! No es pot agafar la longitud de la dada '+v+' correctament')
        return l

    def makeArray(self):
        pos = 0
        self.dataA = []
        for var in self.variables:
            if self.variables[var]['use'] == 'True':  # ha destar plena
                if self.variables[var]['len'] == 1:  # 2 bytes
                    self.dataA.append(self.data[pos])
                    pos += 1
                elif self.variables[var]['len'] == 2:  # 4 bytes
                    self.dataA.append(UintDuint(self.data[pos], self.data[pos + 1]))
                    pos += 2
                else:
                    print('el valor no és ni 1 ni 2')
            else:
                self.dataA.append(0)

    def get_A(self):
        return self.dataA

    def get_B(self):
        return self.dataB

    def upgradeA(self,ar):
        self.dataA = ar

    def __str__(self):
        return str(self.data) + '\n' + str(self.dataA) + '\n' + str(self.dataB)


class UART(Comunication):
    def __init__(self, v, port):  # ordre variables, longitud lectura
        import serial
        super().__init__(v)
        # IOT2040 port = ttyS2
        # IOT2050A port = ttyUSB0
        self.ser = serial.Serial(port=port,
                                 baudrate=9600,
                                 parity=serial.PARITY_NONE,
                                 stopbits=serial.STOPBITS_ONE,
                                 bytesize=serial.SEVENBITS,
                                 timeout=1)

        #Modificaio llongitud directament de la classe pare
        # for v in self.variables:
        #     if v[2]==1:
        #         count += 1
        #     elif v[2] == 2:
        #         count += 2

        self.rang = self.len0

    def readData(self):
        # try:
        self.data = []
        a = str(self.ser.readline())  # La lectura de la UART la passem a string
        if a:
            for j in range(2, self.rang*4,4):
                b = ''
                for k in range(4):
                    b = b + a[j + k]
                self.data.append(int('0x' + b, 16))
        self.upgradeA(self.data)
        return True
        # except:
        #     return False


class Modbus(Comunication):
    def __init__(self, v, host):  # ordre variables, longitud lectura
        from pymodbus.client.sync import ModbusTcpClient
        super().__init__(v)
        self.host = host  # 192.168.250.1
        self.client = ModbusTcpClient(self.host, 502)

    def readData(self):
        self.client.connect()
        try:
            rr = self.client.read_holding_registers(0x000, self.len0 + 3, unit=0)  #adressa, numero , nideia123
            self.data = rr.registers
            if 'scrapIn' in self.variables and 'scrapOut' in self.variables:
                if self.variables['scrapIn']['use'] == "True" and self.variables['scrapOut']['use'] == "True":
                    self.data.pop(9)
                elif self.variables['scrapIn']['use'] == "True" and self.variables['scrapOut']['use'] == "False":
                    for i in [7,7,7]:self.data.pop(i)
                elif self.variables['scrapIn']['use'] == "False" and self.variables['scrapOut']['use'] == "True":
                    for i in [5,5,7]:self.data.pop(i)
                else:
                    for i in [5,5,5,5,5]:self.data.pop(i)
            elif 'scrapIn' in self.variables:
                if self.variables['scrapIn']['use'] == 'True':
                    for i in [7,7,7]:self.data.pop(i)
                else:
                    for i in [5, 5, 5, 5, 5]: self.data.pop(i)
            elif 'scrapOut' in self.variables:
                if self.variables['scrapIn']['use'] == 'True':
                    for i in [5, 5, 7]: self.data.pop(i)
                else:
                    for i in [5, 5, 5, 5, 5]: self.data.pop(i)
            else:
                for i in [5, 5, 5, 5, 5]: self.data.pop(i)

            self.client.close()
            time.sleep(0.5)
        except:
            self.client.close(),
            print('fail to connect Modbus')
        if self.data:
            self.makeArray()
            print(self.dataA)
            return True
        else:
            print('Theres no data')
            return False

class ModbusRTU(Comunication):
    def __init__(self, v,port):
        super().__init__(v)
        from pymodbus.client.sync import ModbusSerialClient as ModbusClient
        os.system("switchserialmode /dev/ttyS2 rs485 --terminate")
        self.client = ModbusClient(method='rtu',
                                   port=port,
                                   stopbits=1,
                                   parity='E',
                                   baudrate=38400,
                                   timeout=0.1,
                                   unit=1
                                   )

    def readData(self):
        self.client.connect()
        try:
            state = self.client.read_holding_registers(address=500, count=1, unit=0x01)
            pIn = self.client.read_holding_registers(address=508, count=2, unit=0x01)
            alarm = self.client.read_holding_registers(address=540, count=2, unit=0x01)
            self.data = [pIn.registers[0], pIn.registers[1], state.registers[0], alarm.registers[0], alarm.registers[0]]
            self.client.close()
        except Exception as e:
            self.client.close()
            print('fail to connect Modbus '+str(e))
        if self.data:
            self.makeArray()
            if self.dataA[2] == 1:
                self.dataA[2] = 2
            elif self.dataA[2] == 2:
                self.dataA[2] = 1
            elif self.dataA[2] == 3:
                self.dataA[2] = 0
            return True
        else:
            print('Theres no data')
            return False


class S7(Comunication):
    def __init__(self, v, host, db):  # ordre variables, longitud lectura
        super().__init__(v)
        # TODO delete port
        self.host = host  # 192.168.250.1
        self.db = db
        self.len0 = self.len0*2
        self.plc = c.Client()


    def readData(self):
        try:
            self.plc.connect(self.host, 0, 1)  # ip
            connection = 1
        except:
            connection = 0
            self.plc.disconnect()
        if connection == 1:
            try:
                self.data = []
                value = self.plc.db_read(self.db, 0, self.len0)  # llegir db (numero_db,primer byte, longitud maxima)
                # agrupem per les variables per bytes
                compt = 0
                for v in self.variables:
                    if self.variables[v]["use"] == "True":
                        if self.variables[v]["len"] == 1:
                            self.data.append(
                                int.from_bytes([value[compt], value[compt + 1]], byteorder='big', signed=False))
                            compt += 2
                        elif self.variables[v]["len"] == 2:
                            self.data.append(
                                int.from_bytes([value[compt], value[compt + 1], value[compt + 2], value[compt + 3]],
                                               byteorder='big', signed=False))
                            compt += 4
                    else:
                        if v == 'pOut':
                            self.data.append(self.data[0])
                        else:
                            pass
                self.plc.disconnect()
                self.dataA = self.data

                return True
            except:
                return False
        else:
            return False

    # def makeArray(self):
    #     '''Com que les variables ja estan concatenades no sha de fer amb el make array'''
    #     pos = 0
    #     self.dataA = []
    #     print(self.variables)
    #     for var in self.variables:
    #         if self.variables[v]["len"] == "True":  # ha destar plena
    #             self.dataA.append(self.data[pos])
    #             pos += 1
    #         elif var[2] == 0:
    #             self.dataA.append(0)


class USB(Comunication):
    def __init__(self, v):  # ordre variables, longitud lectura
        super().__init__(v)
        # import usb.core
        # import usb.util
        # import sys
        # import time
        # import struct

        IDVENDOR = 1424
        IDPRODUCT = 91
        core_con = 0
        while (core_con == 0):
            try:
                self.dev = usb.core.find(idVendor=IDVENDOR, idProduct=IDPRODUCT)
                core_con = 1
                if self.dev is None:
                    raise ValueError('Device not found')
            except:
                core_con = 0

        self.endpoint_in = self.dev[0][(0, 0)][0]
        self.endpoint_out = self.dev[0][(0, 0)][1]
        self.RunPlc()

    def USBwrite(self, msg, revlen):
        x = 0
        cnt1 = 0
        cnt2 = 0
        data = []
        while (1):
            try:
                if x == 0:
                    self.endpoint_out.write(msg)
                    x = 1

                data += self.dev.read(self.endpoint_in.bEndpointAddress, self.endpoint_in.wMaxPacketSize, timeout=20)
                if len(data) == revlen:
                    return data

            except usb.core.USBError as e:

                if e.strerror.find("error sending control message") >= 0:
                    cnt1 += 1
                    if cnt1 > 2:
                        raise ValueError('Over error sending control message')
                    x = 0
                    continue

                elif e.strerror.find("Connection timed out") >= 0:
                    cnt2 += 1
                    if cnt2 > 2:
                        raise ValueError('Over Connection timed out')
                    continue

                else:
                    raise ValueError(e.strerror)

    def PLC_Run_Monitoring(self):
        write_array = [0xAB, 0x00, 0x11, 0x80, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
        write_array.append(randint(1, 0xFF))
        write_array += [0x04, 0x01, 0xFF, 0xFF, 0x02]
        sumcheck = sum(write_array)
        sumHi = ((sumcheck >> 8) & 0xFF)
        sumLo = (sumcheck & 0xFF)
        write_array.append(sumHi)
        write_array.append(sumLo)
        res = self.USBwrite(write_array, 19)
        if len(res) == 19:
            sum1 = res[len(res) - 2] << 8 | res[len(res) - 1]
            res.pop()
            res.pop()
            sum2 = sum(res)
            if sum1 == sum2:
                revc = [171, 0, 16, 192, 0, 2, 0, 0, 251, 0, 0, 0, 4, 1, 0, 0]
                res.pop(12)
                if res == revc:
                    print("PLC is Run Monitoring")
                else:
                    print("PLC Run Monitoring Error")
            else:
                print("PLC Run Monitoring Error")
        else:
            print("PLC Run Monitoring Error")

    def D_Read(self, D_number):
        write_array = [0xAB, 0x00, 0x16, 0x80, 0x00, 0x2, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
        rdm = randint(1, 0xFF)
        write_array.append(rdm)
        write_array += [0x01, 0x04, 0x07, 0x00, 0x00, 0x00, 0x82]
        write_array.append(D_number >> 8);
        write_array.append(D_number & 0xFF);
        write_array += [0x00]
        sumcheck = sum(write_array)
        sumHi = ((sumcheck >> 8) & 0xFF)
        sumLo = (sumcheck & 0xFF)
        write_array.append(sumHi)
        write_array.append(sumLo)
        res = self.USBwrite(write_array, 24)
        if len(res) == 24:
            val1 = res.pop()
            val2 = res.pop()
            sum1 = val2 << 8 | val1
            sum2 = sum(res)
            if sum1 == sum2:
                rdm2 = res.pop(12)
                if rdm == rdm2:
                    val1 = res.pop()
                    val2 = res.pop()
                    value = val2 << 8 | val1
                    val1 = res.pop()
                    val2 = res.pop()
                    revc = [171, 0, 21, 192, 0, 2, 0, 0, 251, 0, 0, 0, 1, 4, 0, 0, 7]
                    if res == revc:
                        return (1, value)
                    else:
                        return (1, value)
                else:
                    return (1, value)
            else:
                return (1, value)
        else:
            return (1, value)

    def RunPlc(self):
        con = 0
        while con == 0:
            self.PLC_Run_Monitoring()
            con = 1

    def readData(self):
        # TODO filtrar de les variables a llegir
        connection = 0
        while(connection == 0):
            try:
                self.data = []
                # lectura pIn, pOut, state
                for i in range(600, 605):
                    self.data.append(self.D_Read(i)[1])
                # lectura alarmes
                for i in range(610, 616):
                    self.data.append(self.D_Read(i)[1])
                self.makeArray()
                if self.variables['pOut']['use'] == "False":
                    self.dataA[1] = self.dataA[0]
                return True
            except:
                try:
                    self.dev = usb.core.find(idVendor=IDVENDOR, idProduct=IDPRODUCT)
                    if self.dev is None:
                        raise ValueError('Device not found')
                    self.RunPlc()
                except:
                    time.sleep(5)




#####################          LECTURA FITXER JSON              #######################
file = open("/home/root/config.json")
dicc_json = json.load(file)
file.close()

            #CONFIGURACIÓ JSON


        # LLISTA PER AVALUAR ELS TIPUS D'INTERRUPCIÓ
flanc = {'rising' : mraa.EDGE_RISING,'falling' : mraa.EDGE_FALLING,'both' : mraa.EDGE_BOTH}
        # LLISTA PER LA CONFIGURACIÓ DI10
dInput10 = {'di0' : 4,'di1' : 5,'di2' : 6,'di3' : 7,'di4' : 8,'di5' : 9,'di6' : 10,'di7' : 11,'di8' : 12,'di9' : 13,'di10' : 14}
        # LLISTA PER LA CONFIGURACIÓ DI5
dInput5 = {'di0' : 12,'di1' : 11,'di2' : 10,'di3' : 9,'di4' : 4}

        #VARIABLE PER GUARDAR LA CONFIGURACIÓ DI
conf_targ = None

        #DICCIONARI PLANTILLA
# dicc_conf = {'pIn': [0, 0], 'pOut': [0, 0], 'state': [0, 0], 'a0': [0, 0], 'a1': [0, 0], 'a2': [0, 0], 'a3': [0, 0], 'a4': [0, 0], 'a5': [0, 0], 'a6': [0, 0], 'a7': [0, 0]}


###########################          PROCESSAMENT JSON             ###########################
        #FUNCIONS ISR

def F_pin(a):           #SUMAR PEÇA ENTRADA
    di10.dataD['pIn'] += 1
    print('suma peça_in')

def F_pout(a):          #SUMAR PEÇA SORTIDA
    di10.dataD['pOut'] +=1
    print('suma peca-out')

def F_state(a):         #ESTAT MÀQUINA
    estat = obj_mraa['state'].read()
    if estat :
        di10.dataD['state'] = 1
    else :
        di10.dataD['state'] = 0
    print('state_scan')

def F_a0(a):            # ESTAT ALARMA 0 o scrap In
    print(dicc_json['variables']['scrapIn']['use'] == 'True')
    if 'scrapIn' in dicc_json['variables']:
        if dicc_json['variables']['scrapIn']['use'] == 'True':
            di10.dataD['scrapIn'] += 1
    else:
        a0 = obj_mraa['a0'].read()
        if a0:
            di10.dataD['a0'] = 1
        else:
            di10.dataD['a0'] = 0
        print('state_a0')

def F_a1(a):            # ESTAT ALARMA 1
    if 'scrapOut' in dicc_json['variables']:
        if dicc_json['variables']['scrapOut']['use'] == 'True':
            di10.dataD['scrapOut'] += 1
    else:
        a1 = obj_mraa['a1'].read()
        if a1:
            di10.dataD['a1'] = 1
        else:
            di10.dataD['a1'] = 0
        print('state_a1')

def F_a2(a):            # ESTAT ALARMA 2
    estat = obj_mraa['a2'].read()
    if estat:
        di10.dataD['a2'] = 1
    else:
        di10.dataD['a2'] = 0
    print('state_a2')
def F_a3(a):            # ESTAT ALARMA 3
    estat = obj_mraa['a3'].read()
    if estat:
        di10.dataD['a3'] = 1
    else:
        di10.dataD['a3'] = 0
    print('state_a3')
def F_a4(a):            # ESTAT ALARMA 4
    estat = obj_mraa['a4'].read()
    if estat:
        di10.dataD['a4'] = 1
    else:
        di10.dataD['a4'] = 0
    print('state_a4')
def F_a5(a):            # ESTAT ALARMA 5
    estat = obj_mraa['a5'].read()
    if estat:
        di10.dataD['a5'] = 1
    else:
        di10.dataD['a5'] = 0
    print('state_a5')
def F_a6(a):            # ESTAT ALARMA 6
    estat = obj_mraa['a6'].read()
    if estat:
        di10.dataD['a6'] = 1
    else:
        di10.dataD['a6'] = 0
    print('state_a6')


# CONFIGURACIÓ DI
# if dicc_json['protocol'] == 'gpio'
if 'targeta' in dicc_json:
    if dicc_json['targeta'] == 'DI10':conf_targ = dInput10
    else: conf_targ = dInput5

    if 'scrapIn' in dicc_json['variables'] and 'a0' in dicc_json['variables']:
        if dicc_json['variables']['scrapIn']['use'] == 'True' and  dicc_json['variables']['a0']['use'] == 'True':
            print("ERROOOR, no es pot posar scrapIn i a0")
            exit()
    if 'scrapOut' in dicc_json['variables'] and 'a1' in dicc_json['variables']:
        if dicc_json['variables']['scrapOut']['use'] == 'True' and dicc_json['variables']['a1']['use'] == 'True':
            print("ERROOOR, no es pot posar scrapIn i a0")
            exit()
    obj_mraa = {}
    print(len(dicc_json['variables']))
    # CARREGUEM EL FITXER JSON A LA PLANTILLA
    for value in dicc_json['variables']:
        if dicc_json['variables'][value]['use']=='True':
            print(value)
            obj_mraa[value] = mraa.Gpio(conf_targ[dicc_json['variables'][value]['port']])  # CONF PORT DEL PIN
            obj_mraa[value].dir(mraa.DIR_IN)  # CONF PORT COM A ENTRADA
            # INICIALITZACIÓ I CONFIGURACIONS GPIO+ISR
            if value == 'pIn':
                obj_mraa[value].isr(flanc[dicc_json['variables'][value]['interrupt']], F_pin, obj_mraa[value])    #INICIALITZEM INTERRUPCIÓ AL PIN CONFIGURAT COM A ENTRADA
            elif value == 'pOut':
                obj_mraa[value].isr(flanc[dicc_json['variables'][value]['interrupt']], F_pout, obj_mraa[value])
            elif value == 'state':
                obj_mraa[value].isr(flanc[dicc_json['variables'][value]['interrupt']], F_state, obj_mraa[value])
            elif value == 'scrapIn':
                obj_mraa[value].isr(flanc[dicc_json['variables'][value]['interrupt']], F_a0, obj_mraa[value])
            elif value == 'scrapOut':
                obj_mraa[value].isr(flanc[dicc_json['variables'][value]['interrupt']], F_a1, obj_mraa[value])
            elif value == 'a0':
                obj_mraa[value].isr(flanc[dicc_json['variables'][value]['interrupt']], F_a0, obj_mraa[value])
            elif value == 'a1':
                obj_mraa[value].isr(flanc[dicc_json['variables'][value]['interrupt']], F_a1, obj_mraa[value])
            elif value == 'a2':
                obj_mraa[value].isr(flanc[dicc_json['variables'][value]['interrupt']], F_a2, obj_mraa[value])
            elif value == 'a3':
                obj_mraa[value].isr(flanc[dicc_json['variables'][value]['interrupt']], F_a3, obj_mraa[value])
            elif value == 'a4':
                obj_mraa[value].isr(flanc[dicc_json['variables'][value]['interrupt']], F_a4, obj_mraa[value])
            elif value == 'a5':
                obj_mraa[value].isr(flanc[dicc_json['variables'][value]['interrupt']], F_a5, obj_mraa[value])
            elif value == 'a6':
                obj_mraa[value].isr(flanc[dicc_json['variables'][value]['interrupt']], F_a6, obj_mraa[value])
            elif value == 'a7':
                obj_mraa[value].isr(flanc[dicc_json['variables'][value]['interrupt']], F_a7, obj_mraa[value])
            else:
                print('senyal no definida')

    print(obj_mraa)

    class DI10():
                                #INICIALITZACIÓ VARIABLES GLOBALS MÀQUINA
        dataD = {"pIn":0, "pOut":0, "state":0,
                "a0":0, "a1":0, "a2": 0,
                "a3" : 0, "a4":0, "a5":0,
                "a6" : 0, "scrapIn": 0, "scrapOut" :0}
        # pIn = 0; pOut = 0; state = 0
        # a0 = 0; a1 = 0; a2 = 0
        # a3 = 0; a4 = 0; a5 = 0
        # a6 = 0; scrapIn = 0; scrapOut =0

        def __init__(self):
            self.data_info = []
            self.dataA = []
                                #LECTURA INICIAL VARIABLES MÀQUINA
            for a in obj_mraa:
                if a == 'state':
                    self.dataD['state'] = obj_mraa[a].read()
                elif a =='a0':
                    self.dataD["a0"] = obj_mraa[a].read()
                elif a =='a1':
                    self.dataD["a1"] = obj_mraa[a].read()
                elif a =='a2':
                    self.dataD["a2"] = obj_mraa[a].read()
                elif a =='a3':
                    self.dataD["a3"] = obj_mraa[a].read()
                elif a =='a4':
                    self.dataD["a4"] = obj_mraa[a].read()
                elif a =='a5':
                    self.dataD["a5"] = obj_mraa[a].read()
                elif a =='a6':
                    self.dataD["a6"] = obj_mraa[a].read()



        def readData(self):     #LECTURA ESTAT VARIABLES MÀQUINA
            print(self.dataD)
            self.dataA = []
            for k in self.dataD:
                if k in dicc_json['variables']:
                    if dicc_json['variables'][k]['use'] == 'True':
                        self.dataA.append(self.dataD[k])
                elif k == 'pOut':
                    self.dataA.append(self.dataD['pIn'])
            return True



    #Creació de l'objecte di10

    di10 = DI10()
