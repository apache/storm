echo [program:storm-$1] | sudo tee -a /etc/supervisor/conf.d/storm-$1.conf
echo command=storm $1 | sudo tee -a /etc/supervisor/conf.d/storm-$1.conf
echo directory=/home/storm | sudo tee -a /etc/supervisor/conf.d/storm-$1.conf
echo autorestart=true | sudo tee -a /etc/supervisor/conf.d/storm-$1.conf
echo user=storm | sudo tee -a /etc/supervisor/conf.d/storm-$1.conf