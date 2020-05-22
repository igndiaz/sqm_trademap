sudo apt-get update
sudo apt-get install python3-pip -y
sudo apt-get install apache2 -y
sudo apt-get install libapache2-mod-wsgi-py3 -y
sudo apt-get install xvfb -y
sudo apt-get install wget -y
sudo apt-get install unzip -y
pip3 install selenium
pip3 install pandas
pip3 install flask
pip3 install pyvirtualdisplay
pip3 install google-cloud-storage
pip3 install openpyxl
sudo update-alternatives --install /usr/bin/python python /usr/bin/python3.7 1 
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
sudo dpkg -i google-chrome-stable_current_amd64.deb
sudo rm google-chrome-stable_current_amd64.deb
sudo chown -R www-data:www-data /var/www
mkdir historico
mkdir historico/src
wget https://chromedriver.storage.googleapis.com/83.0.4103.14/chromedriver_linux64.zip
unzip chromedriver_linux64.zip
sudo rm chromedriver_linux64.zip
chmod +x chromedriver
sudo mv chromedriver historico/src
gsutil cp gs://sqm_trademap/cultivos_trademap_exports.py .
gsutil cp gs://sqm_trademap/cultivos_trademap_imports.py .
gsutil cp gs://sqm_trademap/productos_trademap_exports.py .
gsutil cp gs://sqm_trademap/productos_trademap_imports.py .
chmod +x cultivos_trademap_exports.py cultivos_trademap_imports.py productos_trademap_exports.py productos_trademap_imports.py
mv cultivos_trademap_exports.py historico/src
mv cultivos_trademap_imports.py historico/src
mv productos_trademap_exports.py historico/src
mv productos_trademap_imports.py historico/src
sudo apt --fix-broken install -y
mkdir historico/src/variables_sitios
mkdir historico/src/secrets
gsutil cp gs://sqm_trademap/trademap.json .
mv trademap.json historico/src/secrets
gsutil cp gs://sqm_trademap/trademap_exports.json .
mv trademap_exports.json historico/src/secrets
gsutil cp gs://sqm_trademap/paises.txt .
mv paises.txt historico/src/variables_sitios
gsutil cp gs://sqm_trademap/productos.txt .
mv paises.txt historico/src/variables_sitios
gsutil cp gs://sqm_trademap/productos_cultivos.txt .
mv productos_cultivos.txt historico/src/variables_sitios