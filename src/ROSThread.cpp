#define CSV_IO_NO_THREAD

#include <QMutexLocker>
#include "ROSThread.h"
#include "fast-cpp-csv-parser/csv.h"

using namespace std;

struct PointXYZIRT {
  PCL_ADD_POINT4D;
  float intensity;
  uint32_t t;
  int ring;

  EIGEN_MAKE_ALIGNED_OPERATOR_NEW
}EIGEN_ALIGN16;

POINT_CLOUD_REGISTER_POINT_STRUCT (PointXYZIRT,
                                   (float, x, x) (float, y, y) (float, z, z) (float, intensity, intensity)
                                   (uint32_t, t, t) (int, ring, ring)
                                   )


ROSThread::ROSThread(QObject *parent, QMutex *th_mutex)
  :QThread(parent), mutex_(th_mutex)
{
  processed_stamp_ = 0;
  play_rate_ = 1.0;
  loop_flag_ = false;
  stop_skip_flag_ = true;

  radarpolar_active_ = true;
  imu_active_ = true ;// OFF in v1 (11/13/2019 released), giseop

  search_bound_ = 10;
  reset_process_stamp_flag_ = false;
  auto_start_flag_ = true;
  stamp_show_count_ = 0;
  imu_data_version_ = 0;
  prev_clock_stamp_ = 0;
}


ROSThread::~ROSThread()
{
  data_stamp_thread_.active_ = false;
  gps_thread_.active_ = false;
  imu_thread_.active_ = false;
  ouster_thread_.active_ = false;
  radarpolar_thread_.active_ = false;


  usleep(100000);

  data_stamp_thread_.cv_.notify_all();
  if(data_stamp_thread_.thread_.joinable())  data_stamp_thread_.thread_.join();
  gps_thread_.cv_.notify_all();
  if(gps_thread_.thread_.joinable()) gps_thread_.thread_.join();
  imu_thread_.cv_.notify_all();
  if(imu_thread_.thread_.joinable()) imu_thread_.thread_.join();
  ouster_thread_.cv_.notify_all(); // giseop
  if(ouster_thread_.thread_.joinable()) ouster_thread_.thread_.join();
  radarpolar_thread_.cv_.notify_all(); // giseop
  if(radarpolar_thread_.thread_.joinable()) radarpolar_thread_.thread_.join();

}


void 
ROSThread::ros_initialize(ros::NodeHandle &n)
{
  nh_ = n;

  pre_timer_stamp_ = ros::Time::now().toNSec();
  timer_ = nh_.createTimer(ros::Duration(0.0001), boost::bind(&ROSThread::TimerCallback, this, _1));

  start_sub_  = nh_.subscribe<std_msgs::Bool>("/file_player_start", 1, boost::bind(&ROSThread::FilePlayerStart, this, _1));
  stop_sub_   = nh_.subscribe<std_msgs::Bool>("/file_player_stop", 1, boost::bind(&ROSThread::FilePlayerStop, this, _1));

  clock_pub_ = nh_.advertise<rosgraph_msgs::Clock>("/clock", 1);
  gps_pub_ = nh_.advertise<sensor_msgs::NavSatFix>("/gps/fix", 1000);
  imu_pub_ = nh_.advertise<sensor_msgs::Imu>("/imu/data_raw", 1000);
  magnet_pub_ = nh_.advertise<sensor_msgs::MagneticField>("/mag_data", 1000);
  ouster_pub_ = nh_.advertise<sensor_msgs::PointCloud2>("/os1_points", 1000); // giseop
  radarpolar_pub_ = nh_.advertise<sensor_msgs::Image>("/radar/polar", 10); // giseop
}


void 
ROSThread::run()
{
  ros::AsyncSpinner spinner(0);
  spinner.start();
  ros::waitForShutdown();
}


void 
ROSThread::Ready()
{
  data_stamp_thread_.active_ = false;
  data_stamp_thread_.cv_.notify_all();
  if(data_stamp_thread_.thread_.joinable())  data_stamp_thread_.thread_.join();

  gps_thread_.active_ = false;
  gps_thread_.cv_.notify_all();
  if(gps_thread_.thread_.joinable()) gps_thread_.thread_.join();

  imu_thread_.active_ = false;
  imu_thread_.cv_.notify_all();
  if(imu_thread_.thread_.joinable()) imu_thread_.thread_.join();

  ouster_thread_.active_ = false; // giseop
  ouster_thread_.cv_.notify_all();
  if(ouster_thread_.thread_.joinable()) ouster_thread_.thread_.join();

  radarpolar_thread_.active_ = false; // giseop
  radarpolar_thread_.cv_.notify_all();
  if(radarpolar_thread_.thread_.joinable()) radarpolar_thread_.thread_.join();

  //check path is right or not
  ifstream f((data_folder_path_+"/sensor_data/data_stamp.csv").c_str());
  if(!f.good()){
    cout << "Please check the file path. The input path is wrong (data_stamp.csv not exist)" << endl;
    return;
  }
  f.close();



  //Read CSV file and make map
  FILE *fp;
  int64_t stamp;

  //data stamp data load
  fp = fopen((data_folder_path_+"/sensor_data/data_stamp.csv").c_str(),"r");
  char data_name[50];
  data_stamp_.clear();
  while(fscanf(fp,"%ld,%s\n",&stamp,data_name) == 2){
    data_stamp_.insert( multimap<int64_t, string>::value_type(stamp, data_name));
  }
  cout << "Stamp data are loaded" << endl;
  fclose(fp);

  initial_data_stamp_ = data_stamp_.begin()->first - 1;
  last_data_stamp_ = prev(data_stamp_.end(),1)->first - 1;

  ouster_file_list_.clear();
  radarpolar_file_list_.clear();

  GetDirList(data_folder_path_ + "/sensor_data/Ouster", ouster_file_list_);
  GetDirList(data_folder_path_ + "/sensor_data/radar/polar", radarpolar_file_list_);

  data_stamp_thread_.active_ = true;
  gps_thread_.active_ = true;
  imu_thread_.active_ = true;
  ouster_thread_.active_ = true;
  radarpolar_thread_.active_ = true;

  data_stamp_thread_.thread_ = std::thread(&ROSThread::DataStampThread,this);
  ouster_thread_.thread_ = std::thread(&ROSThread::OusterThread,this);
}


void 
ROSThread::DataStampThread()
{
  auto stop_region_iter = stop_period_.begin();

  for(auto iter = data_stamp_.begin() ; iter != data_stamp_.end() ; iter ++)
  {
    auto stamp = iter->first;
    while((stamp > (initial_data_stamp_+processed_stamp_))&&(data_stamp_thread_.active_ == true))
    {
      if(processed_stamp_ == 0)
      {
        iter = data_stamp_.begin();
        stop_region_iter = stop_period_.begin();
        stamp = iter->first;
      }
      usleep(1);
      if(reset_process_stamp_flag_ == true) break;
      //wait for data publish
    }

    if(reset_process_stamp_flag_ == true)
    {
      auto target_stamp = processed_stamp_ + initial_data_stamp_;
      //set iter
      iter = data_stamp_.lower_bound(target_stamp);
      iter = prev(iter,1);
      //set stop region order
      auto new_stamp = iter->first;
      stop_region_iter = stop_period_.upper_bound(new_stamp);

      reset_process_stamp_flag_ = false;
      continue;
    }

    //check whether stop region or not
    if(stamp == stop_region_iter->first)
    {
      if(stop_skip_flag_ == true)
      {
        cout << "Skip stop section!!" << endl;
        iter = data_stamp_.find(stop_region_iter->second);  //find stop region end
        iter = prev(iter,1);
        processed_stamp_ = stop_region_iter->second - initial_data_stamp_;
      }
      stop_region_iter++;
      if(stop_skip_flag_ == true)
      {
        continue;
      }
    }

    if(data_stamp_thread_.active_ == false)
      return;

    if(iter->second.compare("imu") == 0 && imu_active_ == true)
    {
      imu_thread_.push(stamp);
      imu_thread_.cv_.notify_all();
    }
    else if(iter->second.compare("gps") == 0)
    {
      gps_thread_.push(stamp);
      gps_thread_.cv_.notify_all();
    }
    else if(iter->second.compare("ouster") == 0)
    {
      ouster_thread_.push(stamp);
      ouster_thread_.cv_.notify_all();
    }
    else if(iter->second.compare("radar") == 0 && radarpolar_active_ == true)
    {
      radarpolar_thread_.push(stamp);
      radarpolar_thread_.cv_.notify_all();
    }
    stamp_show_count_++;
    if(stamp_show_count_ > 100)
    {
      stamp_show_count_ = 0;
      emit StampShow(stamp);
    }

    if(prev_clock_stamp_ == 0 || (stamp - prev_clock_stamp_) > 10000000){
      rosgraph_msgs::Clock clock;


      clock.clock.fromNSec(stamp);
      clock_pub_.publish(clock);
      prev_clock_stamp_ = stamp;
    }

    if(loop_flag_ == true && iter == prev(data_stamp_.end(),1))
    {
      iter = data_stamp_.begin();
      stop_region_iter = stop_period_.begin();
      processed_stamp_ = 0;
    }
    if(loop_flag_ == false && iter == prev(data_stamp_.end(),1))
    {
      play_flag_ = false;
      while(!play_flag_)
      {
        iter = data_stamp_.begin();
        stop_region_iter = stop_period_.begin();
        processed_stamp_ = 0;
        usleep(10000);
      }
    }


  }
  cout << "Data publish complete" << endl;
}

void 
ROSThread::TimerCallback(const ros::TimerEvent&)
{
  int64_t current_stamp = ros::Time::now().toNSec();
  if(play_flag_ == true && pause_flag_ == false){
    processed_stamp_ += static_cast<int64_t>(static_cast<double>(current_stamp - pre_timer_stamp_) * play_rate_);
  }
  pre_timer_stamp_ = current_stamp;

  if(play_flag_ == false){
    processed_stamp_ = 0; //reset
    prev_clock_stamp_ = 0;
  }
}


void 
ROSThread::OusterThread()
{
  int current_file_index = 0;
  int previous_file_index = 0;
  while(1)
  {
    std::unique_lock<std::mutex> ul(ouster_thread_.mutex_);
    ouster_thread_.cv_.wait(ul);
    if(ouster_thread_.active_ == false)
      return;
    ul.unlock();

    while(!ouster_thread_.data_queue_.empty())
    {
      auto data = ouster_thread_.pop();

      //publish data
      if(to_string(data) + ".bin" == ouster_next_.first)
      {
        //publish
        ouster_next_.second.header.stamp.fromNSec(data);
        ouster_next_.second.header.frame_id = "ouster"; // frame ID
        if (ouster_pub_.getNumSubscribers() != 0)
          ouster_pub_.publish(ouster_next_.second);
      }
      else
      {
        //load current data
        pcl::PointCloud<PointXYZIRT> cloud;
        cloud.clear();
        sensor_msgs::PointCloud2 publish_cloud;
        string current_file_name = data_folder_path_ + "/sensor_data/Ouster" +"/"+ to_string(data) + ".bin";

        if(find(next(ouster_file_list_.begin(),max(0,previous_file_index-search_bound_)),ouster_file_list_.end(),to_string(data)+".bin") != ouster_file_list_.end())
        {
          ifstream file;
          file.open(current_file_name, ios::in|ios::binary);
          int k = 0;
          while(!file.eof())
          {
            PointXYZIRT point;
            file.read(reinterpret_cast<char *>(&point.x), sizeof(float));
            file.read(reinterpret_cast<char *>(&point.y), sizeof(float));
            file.read(reinterpret_cast<char *>(&point.z), sizeof(float));
            file.read(reinterpret_cast<char *>(&point.intensity), sizeof(float));
            point.ring = (k%64) + 1 ;
            k = k+1 ;
            cloud.points.push_back (point);
          }
          file.close();

          pcl::toROSMsg(cloud, publish_cloud);
          publish_cloud.header.stamp.fromNSec(data);
          publish_cloud.header.frame_id = "ouster";
          if (ouster_pub_.getNumSubscribers() != 0)
            ouster_pub_.publish(publish_cloud);
        }
        previous_file_index = 0;
      }

      //load next data
      pcl::PointCloud<PointXYZIRT> cloud;
      cloud.clear();
      sensor_msgs::PointCloud2 publish_cloud;
      current_file_index = find(next(ouster_file_list_.begin(),max(0,previous_file_index-search_bound_)),ouster_file_list_.end(),to_string(data)+".bin") - ouster_file_list_.begin();
      if(find(next(ouster_file_list_.begin(),max(0,previous_file_index-search_bound_)),ouster_file_list_.end(),ouster_file_list_[current_file_index+1]) != ouster_file_list_.end()){
        string next_file_name = data_folder_path_ + "/sensor_data/Ouster" +"/"+ ouster_file_list_[current_file_index+1];

        ifstream file;
        file.open(next_file_name, ios::in|ios::binary);
        int k = 0;
        while(!file.eof()){
          PointXYZIRT point;
          file.read(reinterpret_cast<char *>(&point.x), sizeof(float));
          file.read(reinterpret_cast<char *>(&point.y), sizeof(float));
          file.read(reinterpret_cast<char *>(&point.z), sizeof(float));
          file.read(reinterpret_cast<char *>(&point.intensity), sizeof(float));
          point.ring = (k%64) + 1 ;
          k = k+1 ;
          cloud.points.push_back (point);
        }

        file.close();
        pcl::toROSMsg(cloud, publish_cloud);
        ouster_next_ = make_pair(ouster_file_list_[current_file_index+1], publish_cloud);
      }
      previous_file_index = current_file_index;
    }
    if(ouster_thread_.active_ == false) return;
  }
}

int 
ROSThread::GetDirList(string dir, vector<string> &files)
{

  files.clear();
  vector<string> tmp_files;
  struct dirent **namelist;
  int n;
  n = scandir(dir.c_str(),&namelist, 0 , alphasort);
  if (n < 0)
  {
    string errmsg{(string{"No directory ("} + dir + string{")"})};
    const char * ptr_errmsg = errmsg.c_str();
    perror(ptr_errmsg);
    // perror("No directory");
  }
  else
  {
    while (n--)
    {
      if(string(namelist[n]->d_name) != "." && string(namelist[n]->d_name) != "..")
      {
        tmp_files.push_back(string(namelist[n]->d_name));
      }
      free(namelist[n]);
    }
    free(namelist);
  }

  for(auto iter = tmp_files.rbegin() ; iter!= tmp_files.rend() ; iter++)
  {
    files.push_back(*iter);
  }
  return 0;
}


void 
ROSThread::FilePlayerStart(const std_msgs::BoolConstPtr& msg)
{
  if(auto_start_flag_ == true){
    cout << "File player auto start" << endl;
    usleep(1000000);
    play_flag_ = false;
    emit StartSignal();
  }
}


void 
ROSThread::FilePlayerStop(const std_msgs::BoolConstPtr& msg)
{
  cout << "File player auto stop" << endl;
  play_flag_ = true;

  emit StartSignal();
}


void 
ROSThread::ResetProcessStamp(int position)
{
  if(position > 0 && position < 10000){
    processed_stamp_ = static_cast<int64_t>(static_cast<float>(last_data_stamp_ - initial_data_stamp_)*static_cast<float>(position)/static_cast<float>(10000));
    reset_process_stamp_flag_ = true;
  }
}

void ROSThread::SaveRosbag() {
  rosbag::Bag bag;
  const std::string bag_path = data_folder_path_ + "/output.bag";
  bag.open(bag_path, rosbag::bagmode::Write);
  cout << "Storing bag to: " << bag_path << endl;

  GetDirList(data_folder_path_ + "/sensor_data/Ouster", ouster_file_list_);

  int current_file_index = 0;
  int previous_file_index = 0;

  cout << "Found: " << ouster_file_list_.size() << " lidar sweeps" << endl;
  int count = 1;
  for (auto &&file_name : ouster_file_list_) {
    cout << "lidar: " << count++ << "/" << ouster_file_list_.size() << endl;

    pcl::PointCloud<PointXYZIRT> cloud;
    cloud.clear();
    sensor_msgs::PointCloud2 publish_cloud;
    string current_file_name =
        data_folder_path_ + "/sensor_data/Ouster/" + file_name;

    ifstream file;
    file.open(current_file_name, ios::in | ios::binary);
    int k = 0;
    while (!file.eof()) {
      PointXYZIRT point;
      file.read(reinterpret_cast<char *>(&point.x), sizeof(float));
      file.read(reinterpret_cast<char *>(&point.y), sizeof(float));
      file.read(reinterpret_cast<char *>(&point.z), sizeof(float));
      file.read(reinterpret_cast<char *>(&point.intensity), sizeof(float));
      point.ring = (k % 64) + 1;
      k = k + 1;
      cloud.points.push_back(point);
    }
    file.close();

    pcl::toROSMsg(cloud, publish_cloud);

    size_t lastindex = file_name.find_last_of(".");
    std::string stamp_str = file_name.substr(0, lastindex);
    int64_t stamp_int;
    std::istringstream(stamp_str) >> stamp_int;

    publish_cloud.header.stamp.fromNSec(stamp_int);
    publish_cloud.header.frame_id = "ouster";
    bag.write("/ouster", publish_cloud.header.stamp, publish_cloud);
  }

  ////////////////////// OPEN GROUND TRUTH FILE /////////////////////

  const std::string gt_csv_path =
      data_folder_path_ + std::string("/global_pose.csv");
  fstream fin;
  fin.open(gt_csv_path, ios::in);
  if (fin.is_open()) {
    cout << "loaded: " << gt_csv_path << endl;

    std::string temp;
    int count = 0;
    nav_msgs::Odometry Tgt_msg;
    Tgt_msg.header.frame_id = "world";
    while (fin >> temp) {
      Eigen::Matrix<double, 4, 4> T = Eigen::Matrix<double, 4, 4>::Zero();
      T(3, 3) = 1.0;

      std::vector<string> row;

      stringstream ss(temp);
      std::string str;
      while (getline(ss, str, ',')) row.push_back(str);
      if (row.size() != 13) break;
      int64_t stamp_int;
      std::istringstream(row[0]) >> stamp_int;
      for (int i = 0; i < 3; i++) {
        for (int j = 0; j < 4; j++) {
          double d = boost::lexical_cast<double>(row[1 + (4 * i) + j]);
          T(i, j) = d;
        }
      }

      Eigen::Affine3d Tgt(T);
      tf::poseEigenToMsg(Tgt, Tgt_msg.pose.pose);
      Tgt_msg.header.stamp.fromNSec(stamp_int);
      bag.write("/groundTruth", Tgt_msg.header.stamp, Tgt_msg);
    }
  }

  cout << "rosbag stored at: " << bag_path << endl;
  bag.close();
}
