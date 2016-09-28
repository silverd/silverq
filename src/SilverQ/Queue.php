<?php

namespace SilverQ;

/**
 * 队列经理人
 *
 * @author JiangJian <silverd@sohu.com>
 * $Id: Queue.php 12 2012-11-19 01:29:52Z jiangjian $
 */

class Queue
{
    /**
     * 设置队列守护进程
     * 死循环弹出队列元素并依次处理
     *
     * @param Com_Queue_Abstract $queue 队列实例
     * @return null
     */
    public static function setDaemon(Com_Queue_Abstract $queue)
    {
        while (true) {

            // 从队列弹出前的一些检测操作
            $queue->prePop();

            // 从队列弹出一个任务（JSON）
            $oneTask = $queue->pop();

            // 队列里没有任务则休息一会儿
            if (! $oneTask) {
                sleep(10);
                continue;
            }

            try {
                // 处理从队列弹出的一个任务
                $result = $queue->postPop($oneTask);
            }
            catch (Exception $e) {
                $result = [
                    'is_ok'      => 0,
                    'return_msg' => $e->getMessage(),
                ];
            }

            // 记录一个任务的处理结果
            $queue->log($oneTask, $result);
        }
    }

    // 重试补救指定队列的失败任务
    // 将 log_queue 中执行失败的任务，弹出并重新push进队列重新排队执行
    public static function retrieve($ids)
    {
        $ok = 0;

        foreach ((array) $ids as $id) {

            if (! $logInfo = Dao('Massive_LogQueue')->get($id)) {
                continue;
            }

            if ($logInfo['is_ok'] == 1) {
                continue;
            }

            // 重新塞回队列
            if (S($logInfo['model_name'])->push($logInfo['org_infos'])) {
                // 然后删除掉该日志
                Dao('Massive_LogQueue')->deleteByPk($id);
                $ok++;
            }
        }

        return $ok;
    }
}