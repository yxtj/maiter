
#include<iostream>
#include<string>
#include<fstream>
#include<vector>
#include <boost/lexical_cast.hpp>
using namespace std;

int Mod(string key,int shards){
    int pos=key.find("_");
    key=key.substr(0,pos);
    pos= boost::lexical_cast<int>(key);
    return (pos % shards);
}
void read_input_file(int shards,int core,string path)
{
    vector<string> buf;
    fstream file;
    string line="  ";
    file.open("input/simrank/1827");
    if(!file)
    {
        cout<<"打开文件失败！"<<endl;
        return;
    }
    float count=0;
    getline(file,line);
    int p=10000;
    while(line!="")
    {
        //cout<<line<<endl;
        count++;
        if(count>p){
        float k= count/16698.8;
        cout<<">>>>>>>>>>>>>>>>>>>>>>>>>>>  "<< k <<"%"<<endl;
        p+=10000;
        }
        int pos=line.find("\t");
        string key=line.substr(0,pos);
        int num=Mod(key,shards);
        string file_num=boost::lexical_cast<string>(num);
        string command="echo \""+line+"\" >> part/part"+file_num;
        system(command.c_str());
        getline(file,line);
    }
    cout<<"节点总数为： "<<count<<endl;
    file.close();
    //string line="  ";
    fstream file1;
    file1.open("conf/mpi-cluster");
    if(!file1)
    {
        cout<<"打开文件失败！"<<endl;
        return;
    }
    getline(file1,line);
    getline(file1,line);
    int i=0;
    while(line!="")
    {
        cout<<line<<endl;
        if(line=="") return;
        int pos=line.find(" ");
        string ip=line.substr(0,pos);
        cout<<"IP:"<<ip<<endl;
        string file_num=boost::lexical_cast<string>(i);
        //string command="ssh "+ip;
        //system(command.c_str());
        //command="mkdir "+path;
        //system(command.c_str());
        //system("exit");
        for(int k=0;k<core;k++){
          string file_num=boost::lexical_cast<string>(i);
          string command= "scp -r part/part"+file_num+" root@"+ip+":"+path;
          system(command.c_str());
          i++;
        }
        getline(file1,line);
    }
    file1.close();
}

int main()
{
  cout<<"请输入待处理文件路径：";
  string path;
  path="/home/wangcl/maiter/input/simrank/";
  cout<<endl;
  cout<<"请输入工作节点核数：";
  int core;
  cin>>core;
  cout<<"请输入机群工作节点总数：";
  int shards;
  cin>>shards;
  shards=shards*core;
  read_input_file(shards,core,path);

  return 0;
}

