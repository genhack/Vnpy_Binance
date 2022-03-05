# vn.py framework BINANCE underlayer interface 

<p align="center">
  <img src ="https://vnpy.oss-cn-shanghai.aliyuncs.com/vnpy-logo.png"/>
</p>

<p align="center">
    <img src ="https://img.shields.io/badge/version-2021.10.27-blueviolet.svg"/>
    <img src ="https://img.shields.io/badge/platform-windows|linux|macos-yellow.svg"/>
    <img src ="https://img.shields.io/badge/python-3.7-blue.svg" />
    <img src ="https://img.shields.io/github/license/vnpy/vnpy.svg?color=orange"/>
</p>

##  illustrate 

 based on the currency exchange API develop ， support in the account under account 、 futures 、 sustainable trade 。

 pay attention to this interface when using ：

1.  only support the full warehouse margin mode 
2.  only supports one-way position mode 

 please at BINANCE use the website to complete the corresponding setting of the account again 。

##  install 

 installation needs based on 2.7.0 version above [VN Studio](https://www.vnpy.com)。

 use directly pip order ：

```
pip install vnpy_binance
```

 after downloading the decompression cmd in operation 

```
python setup.py install
```

##  use 

 start with scripting （script/run.py）：

```
from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import MainWindow, create_qapp

from vnpy_binance import (
    BinanceSpotGateway,
    BinanceUsdtGateway,
    BinanceInverseGateway
)


def main():
    """ main port function """
    qapp = create_qapp()

    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    main_engine.add_gateway(BinanceSpotGateway)
    main_engine.add_gateway(BinanceUsdtGateway)
    main_engine.add_gateway(BinanceInverseGateway)

    main_window = MainWindow(main_engine, event_engine)
    main_window.showMaximized()

    qapp.exec()


if __name__ == "__main__":
    main()
```
