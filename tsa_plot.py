import sys,os,time,datetime,pickle,copy
import numpy as np
#local imports
from vp_data import DataProc,AggScheme,AggFunc,TsArray
from pv_ingest import Utils
from vp_plot import Plotter
from vp_prop import vinepair_creds
###########################
EXPORT_START_DATE="2016-01-01"
EXPORT_END_DATE="2019-12-05"
CHROME_PATH = "/opt/google/chrome/chrome"
###########################
plotter = Plotter(CHROME_PATH)
load_stem = str(sys.argv[1])
tsa = TsArray.load_array(load_stem)
if len(sys.argv) > 2:
    frames = list(str(arg) for arg in sys.argv[2::])
else:
    frames = tsa.arrays.keys()

tsa.summary()
pl = plotter.plot_frames_series(tsa,grouping="byseries",frame_list=frames,series_list=None)
p2 = plotter.plot_frames_series(tsa,grouping="byframe",frame_list=frames,series_list=None)
fig1 = plotter.make_plot(pl,master_title="By Series")
fig2 = plotter.make_plot(p2,master_title="By Frame")

