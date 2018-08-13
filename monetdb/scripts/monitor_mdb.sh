export root_dir="/home/hduser/COE"
export dataset="bfs_livejournal_g4"

if [ z $dataset ]
then
printf "Specify the dataset."
printf "\n\n"
return
fi

export f_path=$root_dir"/benchmark/$dataset.txt"

psrecord `pgrep mserver5` --log $f_path --include-children --duration 180 --interval 1
