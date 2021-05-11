import collections
import functools
import itertools
import json

def readData(filename):
    with open(filename, mode='r', encoding='utf-8') as file:
        for line in file:
            record = json.loads(line)
            yield(record)#ca ne renvoit pas un objet mais un generateur qu'on doit itérer pour récup le contenu stocké dedans


class MapReduce:
    def __init__(self, mapper, reducer, num_workers=0):
        self.m = mapper
        self.r = reducer

    def __call__(self, inputs):
        # appliquer
        map_values = map(self.m, inputs)#A chaque liste de json on associe la fonction mapper() def dans le lab mais on ne l'applique pas
        #ca va retourner un objet map() que l'on va pouvoir itérer et c'est là qu'on va pouvoir appliquer l'opération de mapper sur les inputs
        items = collections.defaultdict(list)#creation d'un dico par defaut où les valeurs seront des listes
        for k,v in itertools.chain.from_iterable(map_values):#on itere sur chacun des éléments de la map creee precedement ce qui va appliquer automatiquement sur chacune des listes du json la fonction mapper
            items[k].append(v) #ca rempli le dico avec k qui est le word et v la clee qui est ici le nom du fichier

        reduce_values = map(self.r, items.items())#dès qu'on itère sur un map ca va appliquer reducer

        return list(reduce_values)
