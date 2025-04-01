package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.client.RedisClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.concurrent.*;

@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    @Qualifier("redisTemplate")
    @Autowired
    private RedisTemplate redisTemplate;

    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024*1024);
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable {
        @Override
        public void run() {
            while(true){
                try {
                    // 1.获取队列中的订单信息
                    VoucherOrder voucherOrder = orderTasks.take();
                    // 2.创建订单
                    handleVoucherOrder(voucherOrder);
                    
                } catch (InterruptedException e) {
                    log.error("订单异常", e);
                }
            }
        }
    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
          //保证库存和一人一单的并发控制在lua脚本中已经通过确保，这里只需要完成订单创建就可以
//        // 1.获取用户
//        Long userId = voucherOrder.getUserId();
//        // 2.创建锁对象
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//        // 3.获取锁
//        boolean isLock = lock.tryLock();
//        // 4.判断是否获取成功
//        if(!isLock){
//            log.error("不允许重复下单");
//            return ;
//        }
//        try {
//            proxy.createVoucherOrder(voucherOrder);
//        }finally {
//            lock.unlock();
//        }
        proxy.createVoucherOrder(voucherOrder);
    }
//    @Override
//    public Result seckellVoucher(Long voucherId) {
//        //查询优惠券
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        //时间判断
//        if(voucher.getBeginTime().isAfter(LocalDateTime.now())){
//            return Result.fail("Voucher killing is not begin.");
//        }
//        if(voucher.getEndTime().isBefore(LocalDateTime.now())){
//            return Result.fail("Voucher killing has ended.");
//        }
//        //判断库存
//        if(voucher.getStock()<1){
//            return Result.fail("Voucher has not enough stock.");
//        }
//
//        Long userId = UserHolder.getUser().getId();
//        //synchronized加锁方式
////        synchronized(userId.toString().intern()) {
////            //@transactional是利用spring的代理对象实现的，如果直接用this调用目标对象无法实现事务，所以要获取代理对象
////            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
////            return proxy.createVoucherOrder(voucherId);
////        }
//
//        //利用Redis实现分布式锁
////        SimpleRedisLock lock = new SimpleRedisLock("order:"+userId, stringRedisTemplate);
//
//        //使用Redisson封装的锁
//        RLock lock = redissonClient.getLock("locker:order:" + userId);
//        boolean success = lock.tryLock();
//        if(!success){
//            return Result.fail("Each user can only buy one voucher.");
//        }
//
//        try {
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        } catch (IllegalStateException e) {
//            throw new RuntimeException(e);
//        } finally {
//            lock.unlock();
//        }
//    }

    private IVoucherOrderService proxy;
    //使用异步查询库存，确认一人一单+订单生成，完成秒杀券抢购
    @Override
    public Result seckellVoucher(Long voucherId) {
        //获取用户
        Long userId = UserHolder.getUser().getId();
        // 1.执行seckill.lua,完成库存和重复订单确认
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString()
        );
        // 2.判断结果
        int r = result.intValue();
        // 2.1.结果不为0，代表没有购买资格
        if(r != 0){
            return Result.fail(r==1?"No voucher left.":"Each user can only buy one voucher.");
        }
        // 2.2.结果为0，将下单信息保存到阻塞队列
        long orderId = redisIdWorker.nextId("order");
        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(orderId);
        voucherOrder.setVoucherId(voucherId);
        voucherOrder.setUserId(UserHolder.getUser().getId());
        orderTasks.add(voucherOrder);

        // 3.为后面的事务特性提前获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        // 3.返回订单id
        return Result.ok(orderId);
    }

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        //一人一单
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();
        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        if (count > 0) {
            log.error("Each user can only buy one voucher.");
        }
        //扣减库存
        boolean success = seckillVoucherService.update().setSql("stock = stock - 1")
                .eq("voucher_id", voucherId).gt("stock", 0)
                .update();
        if (!success) {
            log.error("Voucher has not enough stock.");
        }

        save(voucherOrder);
    }
}
