import snap7.client as c
from random import randint
import os
import mraa
import json

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

def Fprod0(a):           #SUMAR PEÇA ENTRADA
    di10.dataD['production0'] += 1
    # print('suma production0')

def Fprod1(a):          #SUMAR PEÇA SORTIDA
    di10.dataD['production1'] +=1
    # print('suma production1')

def Frej0(a):          #SUMAR PEÇA SORTIDA
    di10.dataD['reject0'] +=1
    # print('suma reject0')

def Frej1(a):          #SUMAR PEÇA SORTIDA
    di10.dataD['reject1'] +=1
    # print('suma reject1')

def Frej2(a):  # SUMAR PEÇA SORTIDA
    di10.dataD['reject2'] += 1
    # print('suma reject2')

def Frej3(a):          #SUMAR PEÇA SORTIDA
    di10.dataD['reject3'] +=1
    # print('suma reject3')

def Frej4(a):          #SUMAR PEÇA SORTIDA
    di10.dataD['reject4'] +=1
    # print('suma reject4')


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
            if value == 'production0':
                obj_mraa[value].isr(flanc[dicc_json['variables'][value]['interrupt']], Fprod0, obj_mraa[value])    #INICIALITZEM INTERRUPCIÓ AL PIN CONFIGURAT COM A ENTRADA
            elif value == 'production1':
                obj_mraa[value].isr(flanc[dicc_json['variables'][value]['interrupt']], Fprod1, obj_mraa[value])
            elif value == 'reject0':
                obj_mraa[value].isr(flanc[dicc_json['variables'][value]['interrupt']], Frej0, obj_mraa[value])
            elif value == 'reject1':
                obj_mraa[value].isr(flanc[dicc_json['variables'][value]['interrupt']], Frej1, obj_mraa[value])
            elif value == 'reject2':
                obj_mraa[value].isr(flanc[dicc_json['variables'][value]['interrupt']], Frej2, obj_mraa[value])
            elif value == 'reject3':
                obj_mraa[value].isr(flanc[dicc_json['variables'][value]['interrupt']], Frej3, obj_mraa[value])
            elif value == 'reject4':
                obj_mraa[value].isr(flanc[dicc_json['variables'][value]['interrupt']], Frej4, obj_mraa[value])
            else:
                print('senyal no definida')

    print(obj_mraa)

    class DI10():
                                #INICIALITZACIÓ VARIABLES GLOBALS MÀQUINA
        dataD = {"production0":0, "production1":0, "reject0":0,
                "reject1":0, "reject2":0, "reject3": 0,
                "reject4" : 0, "NC0":0, "NC1":0,
                "NC2" : 0, "NC3": 0, "NC4" :0}
        # pIn = 0; pOut = 0; state = 0
        # a0 = 0; a1 = 0; a2 = 0
        # a3 = 0; a4 = 0; a5 = 0
        # a6 = 0; scrapIn = 0; scrapOut =0

        def __init__(self):
            self.data_info = []
            self.dataA = []
                                #LECTURA INICIAL VARIABLES MÀQUINA
            for a in obj_mraa:
                if a == 'production0':
                    self.dataD['production0'] = obj_mraa[a].read()
                elif a =='production1':
                    self.dataD["production1"] = obj_mraa[a].read()
                elif a =='reject0':
                    self.dataD["reject0"] = obj_mraa[a].read()
                elif a =='reject1':
                    self.dataD["reject1"] = obj_mraa[a].read()
                elif a =='reject2':
                    self.dataD["reject2"] = obj_mraa[a].read()
                elif a =='reject3':
                    self.dataD["reject3"] = obj_mraa[a].read()
                elif a =='reject4':
                    self.dataD["reject4"] = obj_mraa[a].read()
                else:
                    pass



        def readData(self):     #LECTURA ESTAT VARIABLES MÀQUINA
            self.dataA = []
            for l in range(dicc_json["nOperation"]):
                self.dataA.append(0)
                self.dataA.append(0)
            # print(self.dataD)
            for k in self.dataD:
                if k in dicc_json['variables']:
                    if dicc_json['variables'][k]['use'] == 'True':
                        # print((dicc_json['variables'][k]['operation']-1)*2)
                        if k[:len(k)-1] == 'production':
                            self.dataA[(dicc_json['variables'][k]['operation']-1)*2] += self.dataD[k]
                        elif k[:len(k)-1] == 'reject':
                            self.dataA[((dicc_json['variables'][k]['operation']-1)*2)+1] += self.dataD[k]
                        else:
                            print('algo va mal ')
                            pass
                        # self.dataA.append(self.dataD[k])
            # print(self.dataD, self.dataA)
            return True



    #Creació de l'objecte di10

    di10 = DI10()
