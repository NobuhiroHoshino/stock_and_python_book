import ctypes
import os

#Windowsの再起動
#os.system('shutdown -r -f')

#Windowsの終了
#os.system('shutdown -s -f')

#Windowsのスリープ
ctypes.windll.PowrProf.SetSuspendState(0, 1, 0)

#Windowsの休止
#ctypes.windll.PowrProf.SetSuspendState(1, 1, 0)