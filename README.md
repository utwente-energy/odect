# ODECT
The Open Dynamic Electricity Composition Tracker (ODECT) was developed to create real-time 
insight in the variable environmental impact of electricity consumption. The model collects and models generation and import data for a complete electricity mix. This mix can be converted into environmental impact using impact factors. ODECT v1.0, is focussed dynamic Global Warming Potential (gCO2eq/kWh) of the Dutch electricity mix.

ODECT was developed as part of the master's thesis of Bas Jansen at 
Energy Management Research group of the University of Twente, Enschede, The Netherlands. 
For more information on the group's research, please visit: 
[https://www.utwente.nl/en/eemcs/energy/](https://www.utwente.nl/en/eemcs/energy/)

We believe that we can only transition to a sustainable society through 
transparency and openness. Hence we decided to make our efforts open source, 
such that society can fully benefit from and contribute to science. 
We invite everybody to join us in this cause.

## Knowledge resources
ODECT is developed as part of the master's thesis. 
The thesis is openly available [here](https://essay.utwente.nl/96154/)

## How to cite
When using ODECT for publications, please cite:
-   B. Jansen, "Modelling the dynamic greenhouse gas emission intensity of the Dutch electricity mix", Master's Thesis, Univeristy of Twente, Enschede, the Netherlands. Available [here](https://essay.utwente.nl/96154/)

## Installation

The software requires Python 3.x (3.8 is tested)
([https://www.python.org/](https://www.python.org/)) and depends on the Python libraries 
found in the requirements.txt file.

Install the dependencies: python -m pip install -r requirements.txt

Copy the template settings.py.example and rename into settings.py. Then insert personal API keys for ENTSO-e and KNMI databases in settings/config.py.

## License

This software is made available under the Apache version 2.0 license: https://www.apache.org/licenses/LICENSE-2.0

The software depends on external software and libraries. 
These external packages are likely to contain other software 
which may be licenced under other licenses. 
It is the user's responsibility to ensure that the use of external software and libraries complies with any relevant licenses. A list of used Python libraries can be found in the requirements.txt file

## Contact
In case of any ODECT related questions, please reach out to me:

[Bas Jansen](https://www.linkedin.com/in/b-j-jansen/)
