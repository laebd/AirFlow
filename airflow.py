
import random
import os

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

def hello():
    print("Airflow")

def task_a():
    print(random.uniform(0,100))
    print(random.uniform(0,1000))

def task_c():
    with open('/opt/airflow/dags/file_task_c.txt', 'r') as file:
        data = file.readlines()

    data[-1:] = str(random.uniform(0,100)) + " " + str(random.uniform(0,100))+ "\n"
    with open('/opt/airflow/dags/file_task_c.txt', 'w') as file:
        file.writelines(data)
    #file.writelines(str(random.uniform(0,100)) + " " + str(random.uniform(0,100))+ "\n")[-1:]
    #file.close()

def task_d():
    a = 0.
    b = 0.
    with open('/opt/airflow/dags/file_task_c.txt', "r") as file:
        line = file.readline()
        while line:
            print(line, end="")            
            a = a + float(line.split(' ', 2)[0])
            print(a)
            b = b + float(line.split(' ', 2)[1])
            print(b)
            line = file.readline()
    file.close()
    file = open('/opt/airflow/dags/file_task_c.txt', 'a')
    file.writelines(str(a-b))
    file.close()

with DAG(dag_id="first_dag", start_date=datetime(2022, 12, 13), schedule="40-45 2 * * *", catchup=False, max_active_runs=1) as dag:
    bash_task = BashOperator(task_id="hello", bash_command="echo hello")
    python_task = PythonOperator(task_id="world", python_callable = hello)
    
    #a.      Создайте еще один PythonOperator, который генерирует два произвольных числа и печатает их. Добавьте вызов нового оператора в конец вашего pipeline с помощью >>.
    task_a = PythonOperator(task_id="task_a", python_callable = task_a)

    #b.      Попробуйте снова запустить ваш DAG и убедитесь, что все работает верно.

    #c.      Если запуск произошел успешно, попробуйте изменить логику вашего Python-оператора следующим образом – сгенерированные числа должны записываться в текстовый файл – через пробел. 
    # При этом должно соблюдаться условие, что каждые новые два числа должны записываться с новой строки не затирая предыдущие.
    task_c = PythonOperator(task_id="task_c", python_callable = task_c)

    #d.      Создайте новый оператор, который подключается к файлу и вычисляет сумму всех чисел из первой колонки, 
    # затем сумму всех чисел из второй колонки и рассчитывает разность полученных сумм. 
    # Вычисленную разность необходимо записать в конец того же файла, не затирая его содержимого.
    task_d = PythonOperator(task_id="task_d", python_callable = task_d)

    #e.      Измените еще раз логику вашего оператора из пунктов 12.а – 12.с.
    #  При каждой новой записи произвольных чисел в конец файла, вычисленная сумма на шаге 12.d должна затираться.

    #f.       Настройте ваш DAG таким образом, чтобы он запускался каждую минуту в течение 5 минут.

    
    #bash_task >> python_task >> task_a >> task_c >> task_d
    bash_task >> python_task >> task_c >> task_d

42.907092228395406 64.72991650936684
15.744134272426258 3.6375360660335643
99.11639012787926 26.492630492303014
78.36945677045617 81.88180088962561
22.58743610014663 19.225704180937885
82.01292090481634 77.0512713752
26.514674842361075 31.25986682472053
41.00093019912533 49.59708225121327
93.68931807998486 27.56515236585151
76.06677713623593 57.47451244054117
90.6988039283282 95.69574554811223
20.93772466149052 75.54291523874083
18.870307456023273 71.88095737227269
48.52842853296365 61.11523672064839
72.1920804259377 45.32299933962655
25.101703811436206 50.83433120748466
48.79989361768722 42.168565565572905
8.330630770755665 88.75262387226283
62.25384403884169 79.74225188536062
2.1929551705127115 66.22733479880178
-140.282931868873
