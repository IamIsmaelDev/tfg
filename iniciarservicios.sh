#!/bin/bash

# Deshabilitar Gatekeeper
echo "Deshabilitando Gatekeeper..."
sudo spctl --master-disable

# Iniciar Elasticsearch
echo "Iniciando Elasticsearch..."
cd elasticsearch-8.14.1/bin || { echo "Error: no se pudo cambiar al directorio elastic/elasticsearch-8.14.1/bin"; exit 1; }
nohup ./elasticsearch > ../../elasticsearch.log 2>&1 &

# Esperar unos segundos para asegurar que Elasticsearch ha iniciado
sleep 10

# Iniciar Kibana
echo "Iniciando Kibana..."
cd ../../kibana-8.14.1/bin || { echo "Error: no se pudo cambiar al directorio kibana/kibana-8.14.1/bin"; exit 1; }
nohup ./kibana > ../../kibana.log 2>&1 &

# Esperar unos segundos para asegurar que Kibana ha iniciado
sleep 10

# Habilitar Gatekeeper
echo "Habilitando Gatekeeper..."
sudo spctl --master-enable

echo "Elasticsearch y Kibana se están ejecutando en segundo plano."