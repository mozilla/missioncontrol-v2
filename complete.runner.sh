#!/bin/bash
## This should be run in this folder (i.e. missioncontrol-v2)

# >>> conda initialize >>>
# !! Contents within this block are managed by 'conda init' !!
__conda_setup="$('/home/sguha/anaconda3/bin/conda' 'shell.bash' 'hook' 2> /dev/null)"
if [ $? -eq 0 ]; then
    eval "$__conda_setup"
else
    if [ -f "/home/sguha/anaconda3/etc/profile.d/conda.sh" ]; then
        . "/home/sguha/anaconda3/etc/profile.d/conda.sh"
    else
        export PATH="/home/sguha/anaconda3/bin:$PATH"
    fi
fi
unset __conda_setup
# <<< conda initialize <<<
export PATH=/snap/bin:$PATH
export PYTHONNOUSERSITE=True

## see https://github.com/conda/conda/issues/7980 for activating conda in a bash script
## though might not be needed given the above
## eval "$(conda shell.bash hook)"
conda activate mc2
rm -rf all.the.data.Rdata
result1=`Rscript etl.R`
echo "r1 ${result1}\n"

result2=`Rscript build.models.firefox.desktop.R --debug=0 --out=./all.the.data.Rdata`
echo "r2 ${result2}\n"

result4=`Rscript backup.firefox.desktop.R --data_file=./all.the.data.Rdata  --backup=1`
echo "r4 ${result4}\n"


resukt5=`Rscript produce_and_save_posteriors.R  --backup=1 --data_file=./all.the.data.Rdata --overwrite=1`
echo "r4 ${result4}\n"

## run as sh complete.runner.sh  2>&1 | tee logfile

