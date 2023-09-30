# publishes data (pointclouds, image and bounding boxes of detected targets) to a rosbag file

import rospy
import rosbag
import numpy as np
import PIL.Image
#import pypcd as pcd
from sensor_msgs import point_cloud2
from sensor_msgs.msg import PointCloud2, PointField, Image
import std_msgs.msg
import argparse
import time



# converts a numpy array to a PointCloud2 message
def create_pointcloud(points, fields, header, sensor):
    points_aux = []
    
    if(sensor == 'lidar'):
        for point in points:
            if(not (point[0] == point[1] == point[2])):
                points_aux.append([point[0], point[1], point[2]])
    
    else:
        #aligns the pointclouds of the 2 radars with the camera
        if(sensor == 'radar0'):
            points[:,4]-=0.5
        elif(sensor == 'radar1'):
            points[:,4]+=0.5

        for point in points:
            #points_aux.append([-point[4], -point[2], point[3], point[5], point[6], point[7], point[9]]) #DECENTE
            points_aux.append([-point[2], point[4], point[3], point[5], point[6], point[7], point[9]])


    return point_cloud2.create_cloud(header, fields, points_aux)


if __name__ == '__main__':

    # Create the argument parser
    parser = argparse.ArgumentParser(description="Python script to convert the pointillism dataset to a rosbag file.")
    parser.add_argument('--rosbag-path', default="/home/toalha/Desktop/pointilism dataset/rosbags", help='Path to the directory where the rosbag file will be created')
    parser.add_argument('--scene-path', default="/home/toalha/Desktop/pointilism dataset/data/scene", help='Path to the data folder')
    parser.add_argument('--scene-number', type=int, default=13, help='COnverts a specific Scene')
    parser.add_argument('--start-scene-number', type=int, default=13, help='Starts converting from a specific Scene')
    parser.add_argument('--end-scene-number', type=int, default=13, help='Ends converting in a specific Scene')
    parser.add_argument('--run-all-scenes', type=bool, default=False, help='Converts all scenes (13-60)')
    parser.add_argument('--run-ros-node', type=bool, default=False, help='Run the ROS node')
    args = parser.parse_args()

    #path to the rosbag file and the scene
        # Access and use the argument values
    rosbag_path = args.rosbag_path
    scene_path = args.scene_path
    start_scene_number = args.start_scene_number
    end_scene_number = args.end_scene_number
    scene_number = args.scene_number
    run_ros_node = args.run_ros_node
    run_all_scenes = args.run_all_scenes

    if(scene_number != 13):
        start_scene_number = scene_number
        end_scene_number = scene_number

    if(start_scene_number >= end_scene_number):
        print("Error: no end scene, specified. Program will convert just the start scene.\n")
        end_scene_number = start_scene_number

    if(run_ros_node):
        print("Running the ROS node.\n")
        # Initialize the ROS Node
        rospy.init_node('pointillism_node')

    if(run_all_scenes):
        end_scene_number = 60
 

    for scene_number in range(start_scene_number, end_scene_number + 1):
        scene_name = "scene" + str(scene_number)


        #creates the rosbag file
        bag = rosbag.Bag(rosbag_path + "/" + scene_name + ".bag", 'w')

        # Create the fields for the point cloud data
        fields_radar = [
            PointField('x', 0, PointField.FLOAT32, 1),
            PointField('y', 4, PointField.FLOAT32, 1),
            PointField('z', 8, PointField.FLOAT32, 1),
            PointField('range', 12, PointField.FLOAT32, 1),
            PointField('velocity', 16, PointField.FLOAT32, 1),
            PointField('doppler', 20, PointField.FLOAT32, 1),
            PointField('intensity', 24, PointField.FLOAT32, 1)
        ]

        fields_lidar = [
            PointField('x', 0, PointField.FLOAT32, 1),
            PointField('y', 4, PointField.FLOAT32, 1),
            PointField('z', 8, PointField.FLOAT32, 1)
        ]

        if(run_ros_node):
            #creates the publishers
            pub_radar0 = rospy.Publisher('/radar_0', PointCloud2, queue_size=1)
            pub_radar1 = rospy.Publisher('/radar_1', PointCloud2, queue_size=1)
            pub_lidar = rospy.Publisher('/lidar', PointCloud2, queue_size=1)
            pub_image = rospy.Publisher('/camera/image_color', Image, queue_size=1)


        #publishes the pointclouds
        for i in range(295):
            frame_name = ""
            if i < 10:
                frame_name = "00000" + str(i)
            elif i < 100:
                frame_name = "0000" + str(i)
            else:
                frame_name = "000" + str(i)

            #reads the image from the scene
            img = PIL.Image.open(scene_path + str(scene_number) + "/images/" + frame_name + ".jpg")

            # Convert the Pillow image to a NumPy array
            img_np = np.array(img)

            # Create a ROS Image message and populate it
            ros_image = Image()
            if(run_ros_node):
                ros_image.header.stamp = rospy.Time.now()
            else:
                #simulates the time of the image at 30 FPS
                ros_image.header.stamp = rospy.Time.from_sec(time.time())
            ros_image.height = img_np.shape[0]
            ros_image.width = img_np.shape[1]
            ros_image.encoding = 'rgb8'  # Adjust the encoding as needed (e.g., 'rgb8' for RGB)
            ros_image.data = img_np.tobytes()


            #reads the pointclouds from the scene
            data_radar0 = np.genfromtxt(scene_path + str(scene_number) + "/radar_0/" + frame_name + ".csv", delimiter=',', skip_header=1)
            data_radar1 = np.genfromtxt(scene_path + str(scene_number) + "/radar_1/" + frame_name + ".csv", delimiter=',', skip_header=1)
            data_lidar = np.genfromtxt(scene_path + str(scene_number) + "/lidar/" + frame_name + ".pcd", delimiter=' ', skip_header=13)
            #data_lidar =  pcd.PointCloud.from_path(scene_path + str(scene_number) + "/lidar/" + frame_name + ".pcd").pc_data

            # Create the header for the message
            header = std_msgs.msg.Header()
            if(run_ros_node):
                header.stamp = rospy.Time.now()
            else:
                if(i == 0):
                    start_time = time.time()
                    header.stamp = rospy.Time.from_sec(start_time)
                else:
                    header.stamp = rospy.Time.from_sec(start_time + i/30)
            header = std_msgs.msg.Header()
            header.frame_id = "pointillism_ego_vehicle"

            #creates the pointclouds
            pointcloud_radar0 = create_pointcloud(data_radar0, fields_radar, header, 'radar0')
            pointcloud_radar1 = create_pointcloud(data_radar1, fields_radar, header, 'radar1')
            pointcloud_lidar = create_pointcloud(data_lidar, fields_lidar, header, 'lidar')

            if(run_ros_node):
                #publishes the pointclouds
                pub_radar0.publish(pointcloud_radar0)
                pub_radar1.publish(pointcloud_radar1)
                pub_lidar.publish(pointcloud_lidar)
                pub_image.publish(ros_image)

            #saves the pointclouds to the rosbag file
            bag.write('radar_0', pointcloud_radar0)
            bag.write('radar_1', pointcloud_radar1)
            bag.write('lidar', pointcloud_lidar)
            bag.write('camera/image_color', ros_image)

            #prints the progress
            if(start_scene_number != end_scene_number):
                print("Frame " + str(i) + " of 295 from scene " + str(scene_number) + " published.\n")
            else:
                print("Frame " + str(i) + " of 295 published.\n")

        #closes the rosbag file
        bag.close()
