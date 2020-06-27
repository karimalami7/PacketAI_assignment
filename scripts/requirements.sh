#####check python
if ! [ -x "$(command -v python3)" ]; then
	echo '*******************install python3*******************'
	apt-get  install python3
else 
	echo '*******************python3 exists*******************'
fi

####check pip3
if ! [ -x "$(command -v pip3)" ]; then
	echo '*******************install pip3*******************'
	python3 -m pip install --upgrade pip

else 
	echo '*******************pip3 exists*******************'
fi

####check virtualenv
if ! [ -x "$(command -v virtualenv)" ]; then
	echo '*******************install virtualenv*******************'
	
	pip3 install virtualenv

else 
	echo '*******************virtualenv exists*******************'
fi

echo '*************create P_AI virtualenv*******************'

virtualenv -p python3 P_AI
