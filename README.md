# oott-ships-db
Vessels DB creation and monitoring

Monitored Area

Using date from http://www.birdtheme.org/useful/v3tool.html
and http://globalenergyobservatory.org/constructNetworkIndex.php

Houston: -95.45,29.83 / -93.60,28.70
Corpus Christi: -96.20,26.90 / -97.60,28.30
New Orleans: -91.00,30.5 / -88.30,28.85
Los Angeles: -117.80,33.46 / -118.60,34.00

Requests:
https://www.marinetraffic.com/en/ais/get_info_window_json?asset_type=ship&id=713758
https://www.marinetraffic.com/map/get_data_json/sw_x:-96/sw_y:28/ne_x:-92/ne_y:31/zoom:9/station:0

{"imo":"9713416","name":"AQUAPROSPER",
"type":"Bulk Carrier","t":"1473496265",
"sar":false,"dest":"PANAMA CANAL",
"etastamp":"Sep 14, 07:00",
"ship_speed":12.7,"ship_course":132.2,
"timestamp":"Sep 10, 2016 08:31 UTC",
"__id":"118781","pn":"9713416-636016791-e8752aaa2625addfa23cd0fa02e08897",
"vo":0,"ff":false,
"direct_link":"\/vessels\/AQUAPROSPER-IMO-9713416-MMSI-636016791",
"draught":12.2,"year":"2015","gt":"34830"}


16677925	-53144039	1322	127	270	636016791	AQUAPROSPER	0

71014

------------------
Filtering criteria:
------------------

>>> vessels['Width'][(vessels['Length'] < 400) & (vessels['GT'] > 80000)].describe()
count    1631.000000
mean       52.705702
std         6.743720
min        12.000000
25%        48.000000
50%        50.000000
75%        60.000000
max        78.000000
Name: Width, dtype: float64

>>> vessels['GT'][(vessels['Length'] > 250) & (vessels['GT'] > 80000)].sum() / vessels['GT'].sum()
0.30458460971009638

-------------------
"What is the conversion rate from DWT to barrels?"

DWT stands for "Deadweight Tonnes". The deadweight of a ship is the 
weight needed to submerge  her from her light draft to her fully loaded 
draft. This is not the cargo capacity, because the  ship will also be 
carrying fuel, stores, crew, water, etc etc etc. A 20,000 DWT tanker 
will probably have a cargo capacity of around 16,000 Tonnes

The capacity in barrels will depend on the gravity of the cargo being 
carried. There are two ways to figure that. Weight in Tonnes divided by 
density will give you cubic metres, which can be multiplied by 6.28981 
to give you barrels. Alternatively, Tonnes divided by API Table 13 will 
give you barrels directly. 

Table 13 ranges from about 0.11 for a light gasoline, to about 0.16 for 
marine fuel oil. 

So your 20,000 DWT tanker can carry anything from about 100,000 Barrels 
of MFO to about 145,000 Barrels of gasoline.

-------------------
It will depend on the density of oil, which varies somewhat according to
 what field it is from. If I take 0.85 kg/L as a typical density, and I 
 assume the deadweight tonnahe is in metric tonnes,
 20000 t x 1 m³/0.85 t = 23530 m³

A barrel of oil is 42 US gallons or 0.159 m³, so 147980 barrels 
approximately. A little more for light crude, a little less for heavy 
crude.

-------------------
2 Types: GT < 120, GT >= 120
