# run.ps1

$ErrorActionPreference = "Stop"

# Chemin du projet
$projectPath = "$env:USERPROFILE\OneDrive\Desktop\Auto_formation\data_upskilling\projet-end-to-end\Data-pipeline"
Set-Location $projectPath

# JAVA_HOME avec ta version d'Adoptium
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-21.0.7.6-hotspot"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"

Write-Host "[INFO] JAVA_HOME défini sur : $env:JAVA_HOME" -ForegroundColor Green

# Activation du venv
Write-Host "[INFO] Activation de l'environnement virtuel..." -ForegroundColor Green
.\venv\Scripts\Activate.ps1

# Variables Spark/Hadoop
Write-Host "[INFO] Configuration des variables Hadoop/Spark..." -ForegroundColor Green
$env:HADOOP_HOME = "F:\hadoop"
$env:HADOOP_CONF_DIR = "F:\hadoop\etc\hadoop"
$env:HADOOP_USER_NAME = "hadoopuser"

# Lancement du script Python + options Java directement
Write-Host "[INFO] Démarrage du script data_loader.py avec options JVM..." -ForegroundColor Green

# ⚠️ Ne PAS utiliser JAVA_TOOL_OPTIONS ici
# On lance python avec _JAVA_OPTIONS directement
$_JAVA_OPTIONS = "-Djdk.module.addReads=java.base=ALL-UNNAMED -Djdk.module.addOpens=java.base/java.security=ALL-UNNAMED -Djdk.module.addOpens=java.base/javax.security.auth=ALL-UNNAMED"

# Et on exécute le script avec ces options
cmd /c "$_JAVA_OPTIONS && python .\src\data_loader.py"