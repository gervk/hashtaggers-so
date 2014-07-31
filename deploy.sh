#!/bin/bash

git clone https://github.com/sisoputnfrba/so-commons-library
git clone https://github.com/sisoputnfrba/ansisop-parser
git clone https://github.com/sisoputnfrba/scripts-ansisop

# Instalo las commons
cd so-commons-library/
sudo make install
cd ..
# Instalo las parser-ansisop
cd ansisop-parser/parser/
sudo make all
sudo make install
cd ..
cd ..

# Declaro variables de entorno
export LD_LIBRARY_PATH=/home/utnso/tp-2014-1c-hashtaggers/
echo $LD_LIBRARY_PATH
export ANSISOP_CONFIG=./programa/programa_config
echo $ANSISOP_CONFIG
sudo ln -s /home/utnso/tp-2014-1c-hashtaggers/programa.ansisop /usr/bin/ansisop

# Creo el proyecto ansisop
sudo make allc

# Hago ejecutables los scripts
cd scripts-ansisop

chmod +x ./scripts/completo.ansisop
chmod +x ./scripts/consumidor.ansisop
chmod +x ./scripts/deref-prioridad.ansisop
chmod +x ./scripts/facil.ansisop
chmod +x ./scripts/fibo.ansisop
chmod +x ./scripts/for.ansisop
chmod +x ./scripts/forES.ansisop
chmod +x ./scripts/mcd.ansisop
chmod +x ./scripts/pesado.ansisop
chmod +x ./scripts/productor.ansisop
chmod +x ./scripts/segfault.ansisop
chmod +x ./scripts/stackoverflow.ansisop
chmod +x ./scripts/vector.ansisop
chmod +x ./scripts/bench/productor.ansisop
chmod +x ./scripts/bench/starter.ansisop