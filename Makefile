SMALL = /scratch/cs/socialmediadata/db6.sqlite3
small1:
	sbatch -c 12 --mem=50G --time=0-5 --wrap "/usr/bin/time -v python load-queue.py --readers=3 --decoders=8 ${SMALL} /scratch/cs/socialmediadata/data/reddit-subs-2005-2022/reddit/subreddits/{AskALiberal,AskBernieSupporters,AskConservatives,AskDemocrats,AskLibertarians,AskThe_Donald,AskTrumpSupporters,BannedFromThe_Donald,Conservative,EnoughTrumpSpam,Fuckthealtright,GunsAreCool,HillaryForAmerica,HillaryForPrison,Liberal,Libertarian,POTUSWatch,Republican,SandersForPresident,ShitLiberalsSay,ShitPoliticsSays,The_Donald,TrumpCriticizesTrump,Trumpgret,WayOfTheBern,altright,askaconservative,askhillarysupporters,conservatives,conspiracy,conspiratard,democrats,guns,hillaryclinton,liberalgunowners,news,politics,progressive,progun,PoliticalDiscussion,moderatepolitics,changemyview}_submissions.zst    ; sleep 120 ;    /usr/bin/time -v python load-queue.py --readers=3 --decoders=8 ${SMALL} /scratch/cs/socialmediadata/data/reddit-subs-2005-2022/reddit/subreddits/{AskALiberal,AskBernieSupporters,AskConservatives,AskDemocrats,AskLibertarians,AskThe_Donald,AskTrumpSupporters,BannedFromThe_Donald,Conservative,EnoughTrumpSpam,Fuckthealtright,GunsAreCool,HillaryForAmerica,HillaryForPrison,Liberal,Libertarian,POTUSWatch,Republican,SandersForPresident,ShitLiberalsSay,ShitPoliticsSays,The_Donald,TrumpCriticizesTrump,Trumpgret,WayOfTheBern,altright,askaconservative,askhillarysupporters,conservatives,conspiracy,conspiratard,democrats,guns,hillaryclinton,liberalgunowners,news,politics,progressive,progun,PoliticalDiscussion,moderatepolitics,changemyview}_comments.zst --comments    ;   sleep 120  ;   TMPDIR=/scratch/cs/socialmediadata/ time srun python load-queue.py --index ${SMALL} -"

SMALL2_SOURCE = /scratch/cs/socialmediadata/data/reddit-subs-2005-06--2023-12/reddit/subreddits23/{AskALiberal,AskBernieSupporters,AskConservatives,AskDemocrats,AskLibertarians,AskThe_Donald,AskTrumpSupporters,BannedFromThe_Donald,Conservative,EnoughTrumpSpam,Fuckthealtright,GunsAreCool,HillaryForAmerica,HillaryForPrison,Liberal,Libertarian,POTUSWatch,Republican,SandersForPresident,ShitLiberalsSay,ShitPoliticsSays,The_Donald,TrumpCriticizesTrump,Trumpgret,WayOfTheBern,altright,askaconservative,askhillarysupporters,conservatives,conspiracy,conspiratard,democrats,guns,hillaryclinton,liberalgunowners,news,politics,progressive,progun,PoliticalDiscussion,moderatepolitics,changemyview}
SMALL2 = /scratch/cs/socialmediadata/processed/db-small2-xiay5.sqlite3
small2:
	sbatch -c 12 --mem=50G --time=0-5 --job-name smd-small2 --wrap "rm -f ${SMALL2}.new    ;    /usr/bin/time -v python load-queue.py --readers=3 --decoders=8 ${SMALL2}.new ${SMALL2_SOURCE}_submissions.zst    ; sleep 10 ;    /usr/bin/time -v python load-queue.py --comments --readers=3 --decoders=8 ${SMALL2}.new ${SMALL2_SOURCE}_comments.zst    ; sleep 10 ;    TMPDIR=/scratch/cs/socialmediadata/processed/ /usr/bin/time -v python load-queue.py --index ${SMALL2}.new -    ;    mv ${SMALL2}.new ${SMALL2}"

SMALL3_SOURCE = /scratch/cs/socialmediadata/data/reddit-subs-2005-06--2023-12/reddit/subreddits23/
SMALL3 = /scratch/cs/socialmediadata/processed/db-small3-xiay5.sqlite3
SMALL3_SUBS=subs-small3.txt
small3:
#	sbatch -c 6 --mem-per-cpu=7G --time=5-0 --job-name smd-small3 -o slurm-small3-%j.out --wrap "set -x    ;    rm -f ${SMALL3}.new    ;    /usr/bin/time -v python load-queue.py ${SMALL3}.new ${SMALL3_SOURCE}/ --sub-list=${SMALL3_SUBS} --chunk-lines=100000    ; sleep 10 ; cp -p ${SMALL3}.new ${SMALL3}.new2  ;     /usr/bin/time -v python load-queue.py --comments ${SMALL3}.new2 ${SMALL3_SOURCE}/ --sub-list=${SMALL3_SUBS} --chunk-lines=100000"
	srun --time=0-12 --mem=10G --pty rsync ${SMALL3}.new2 ${SMALL3}.new3
	sbatch --wait --constraint='csl|skl|milan' -c 2 --mem-per-cpu=50G --time=5-0 --job-name smd-small3-index -o slurm-small3-index-%j.out --wrap "set -x    &&    export TMPDIR=/scratch/cs/socialmediadata/processed/    &&    /usr/bin/time -v python load-queue.py --index ${SMALL3}.new3 -    &&    mv ${SMALL3}.new3 ${SMALL3}"


big1:
	srun --pty -c 18 --mem=25G --time=5-0 /usr/bin/time -v python load-queue.py --readers=2 --decoders=14 /scratch/cs/socialmediadata/db-all.sqlite3 '/scratch/cs/socialmediadata/data/reddit-subs-2005-2022/reddit/subreddits/*_submissions.zst' --thin
	srun --pty -c 18 --mem=25G --time=5-0 /usr/bin/time -v python load-queue.py --readers=2 --decoders=14 /scratch/cs/socialmediadata/db-all.sqlite3 '/scratch/cs/socialmediadata/data/reddit-subs-2005-2022/reddit/subreddits/*_comments.zst' --comments --thin


BIG2_SOURCE = /scratch/cs/socialmediadata/data/reddit-subs-2005-06--2023-12/reddit/subreddits23/
BIG2 = /scratch/cs/socialmediadata/processed/db-big2-xiay5.sqlite3
big2:
#	sbatch -c 30 --mem-per-cpu=4G --time=5-0 --job-name smd-big2 -o slurm-big2-%j.out --wrap "set -x    ;    rm -f ${BIG2}.new    ;    /usr/bin/time -v python load-queue.py --thin --readers=4 --decoders=30 ${BIG2}.new ${BIG2_SOURCE}/\*_submissions.zst    ; sleep 10 ;    /usr/bin/time -v python load-queue.py --comments --thin --readers=2 --decoders=30 ${BIG2}.new ${BIG2_SOURCE}/\*_comments.zst"
	sbatch -c 2 --mem-per-cpu=5G --time=5-0 --job-name smd-big2-index -o slurm-big2-%j.out --wrap "set -x    ;    TMPDIR=/scratch/cs/socialmediadata/processed/ /usr/bin/time -v python load-queue.py --index ${BIG2}.new -    ;    mv ${BIG2}.new ${BIG2}"


BIG3_SOURCE = /scratch/cs/socialmediadata/data/reddit-subs-2005-06--2023-12/reddit/subreddits23/
BIG3 = /scratch/cs/socialmediadata/processed/db-big3-xiay5.sqlite3
big3:
#	#sbatch -c 10 --mem-per-cpu=7G --time=5-0 --job-name smd-big3 -o slurm-big3-%j.out --wrap "set -x    ;    rm -f ${BIG3}.new    ;    /usr/bin/time -v python load-queue.py ${BIG3}.new ${BIG3_SOURCE}/ --sub-list='*' --chunk-lines=100000    ; sleep 10 ; cp -p ${BIG3}.new ${BIG3}.new2  ;     /usr/bin/time -v python load-queue.py --comments ${BIG3}.new2 ${BIG3_SOURCE}/ --sub-list='*' --chunk-lines=100000"
#	srun --time=0-12 --pty "rsync ${BIG3}.new2 ${BIG3}.new3"
	sbatch --constraint='csl|skl|milan' -c 2 --mem-per-cpu=50G --time=5-0 --job-name smd-big3-index -o slurm-big3-index-%j.out --wrap "set -x    &&    export TMPDIR=/scratch/cs/socialmediadata/processed/    &&    /usr/bin/time -v python load-queue.py --index ${BIG3}.new3 -    &&    mv ${BIG3}.new3 ${BIG3}"



DUCKDB_TEST = /scratch/cs/socialmediadata/processed/db-duckdb-test.duck
duckdb-test:
#	#ID1=$$(
	sbatch --time=0-5 -c 5 --mem-per-cpu=7G --job-name smd-duckdb-test -o slurm-duckdb-test-%j.out --parsable --wrap "set -x    ;    source /home/darstr1/sys/venv-duckdb/bin/activate    ;    rm -f ${DUCKDB_TEST}.new    ;    srun /usr/bin/time -v python3 duckdb/reddit_to_duckdb.py ${DUCKDB_TEST}.new ${SMALL2_SOURCE}_{submissions,comments}.zst --batchsize=1000"
#) ; \
	sbatch --dependency=afterok:$$ID1 --time=5-0 -c 2 --mem-per-cpu=5G --job-name smd-duckdb-test-idx -o slurm-duckdb-test-%j.out --wrap "set -x    ;    source /home/darstr1/sys/venv-duckdb/bin/activate    ;    srun /usr/bin/time -v python3 duckdb/reddit_to_duckdb.py --index ${DUCKDB_TEST}.new -    ;     mv ${DUCKDB_TEST}.new ${DUCKDB_TEST}"
