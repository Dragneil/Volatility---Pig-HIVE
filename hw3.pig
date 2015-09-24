/*//multiple files
A = LOAD '/home/krishna/small/*.csv' USING PigStorage(',','-tagFile');
//single file
A = LOAD '/home/krishna/small/AAPL.csv' USING PigStorage(',','-tagFile');
*/

A = LOAD '/home/krishna/small/*.csv' USING PigStorage(',','-tagFile');
Data = FOREACH A GENERATE (chararray)$0 as filename, (chararray)$1 as Date,(double)$7 as AdjClose;
B = FILTER Data BY Date != 'Date';
C = FOREACH B GENERATE filename, STRSPLIT(Date,'-'), AdjClose;
Data = FOREACH C GENERATE filename,(int) $1.$0 as year,(int)$1.$1 as month,(int) $1.$2 as date, AdjClose;
grouped = GROUP Data BY (filename, year, month);
G = FOREACH grouped{E = ORDER Data by $1,$2,$3 DESC;
					I = LIMIT E 1;
					F = ORDER Data by $1,$2,$3 ASC;
					J = LIMIT F 1;
					GENERATE group, FLATTEN(J.AdjClose) as beginning, FLATTEN(I.AdjClose) as ending;};
diff = FOREACH G GENERATE group, FLATTEN((ending - beginning) / beginning) as xi;
xigroups = GROUP diff BY group.filename;
avg = FOREACH xigroups GENERATE group as filename, FLATTEN(diff.xi) as xi, FLATTEN(AVG(diff.xi)) as mean, COUNT(diff) AS N;

sq = FOREACH avg GENERATE filename, FLATTEN((xi-mean)*(xi-mean)) as square, N;
Volatility = FOREACH (GROUP sq BY filename) GENERATE group as filename, FLATTEN(SUM(sq.square)) as summed, FLATTEN(sq.N) as N;
vol = DISTINCT Volatility;
FINALVOL = FILTER vol BY summed > 0;
FINALVOL = FILTER FINALVOL BY N != 0;

FINALVOL = FOREACH FINALVOL GENERATE filename as filename, FLATTEN(SQRT(summed/(N-1))) as volatility;

MAX = ORDER FINALVOL by $1 DESC;
MAX10 = LIMIT MAX 10;
MIN = ORDER FINALVOL by $1 ASC;

MIN10 = LIMIT MIN 10;

LIST = UNION MAX10, MIN10;
STORE LIST INTO '/home/krishna/Desktop/DIC/hw3';
