{-# LANGUAGE ScopedTypeVariables, BangPatterns, ViewPatterns, TypeApplications,
   MultiParamTypeClasses #-}
module Pure.Locker where

import Control.Concurrent
import Control.Concurrent.STM

import Data.Map

import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception (SomeException)
import Control.Monad
import Control.Monad.Catch
import Control.Monad.IO.Class

import Data.List as List
import Data.Map as Map

type Locker k v = TMVar (Map k (Int,Maybe (TMVar v)))

-- TODO: Rewrite withMany_ to be more resilient in the face of 
-- acquisition/release failures.

-- Be sure that acquire and release do not throw exceptions or withMany_ could
-- fail and cause other threads using the shared `Locker` to deadlock.
class Resource k v where
  acquire :: k -> IO v
  release :: k -> v -> IO ()

-- | Construct an empty `Locker`
newLocker :: Ord k => IO (Locker k v)
newLocker = newTMVarIO mempty
{-# INLINE newLocker #-}

acquireMany :: forall k v a m. (MonadIO m, Ord k) => (k -> m v) -> Locker k v -> [k] -> m (Map k v)
acquireMany acquire locker_ (List.nub -> keys) = 
  acquireAllTMVars >>= acquireAllVars 
  where
    -- Acquire all necessary TMVars
    acquireAllTMVars :: m [(k,TMVar v)]
    acquireAllTMVars = mapM acquireVar keys 
      where
        acquireVar :: k -> m (k,TMVar v)
        acquireVar k = do
          r <- liftIO $ atomically $ do
            locker <- takeTMVar locker_
            case Map.lookup k locker of
              Just (n,Nothing) -> retry
              Just (n,Just tv) -> do
                putTMVar locker_ $! Map.insert k (n + 1,Just tv) locker
                pure (Right tv)
              Nothing -> do
                tv <- newEmptyTMVar
                putTMVar locker_ $! Map.insert k (1,Just tv) locker
                pure (Left tv)
          case r of
            Right tv -> pure (k,tv)
            Left  tv -> do
              v <- acquire k
              liftIO $ atomically (putTMVar tv v)
              pure (k,tv)

    -- Take all values in a single transaction, restarting as necessary
    -- until they're all available. This is where contention can become an
    -- issue and cause delays in completion. Other threads should be productive
    -- when one thread is having trouble in phase2.
    acquireAllVars :: [(k,TMVar v)] -> m (Map k v)
    acquireAllVars = fmap Map.fromList . liftIO . atomically . mapM acquireVals
      where
        acquireVals (k,tv) = do
          v <- takeTMVar tv
          pure (k,v)

releaseMany :: forall m k v. (MonadIO m, Ord k) => (k -> v -> m ()) -> Locker k v -> Map k v -> m ()
releaseMany release locker_ = releaseAll . Map.toList
  where
    -- Put resources back in locker or release them if no longer required.
    releaseAll :: [(k,v)] -> m ()
    releaseAll = mapM_ releaseVal 
      where
        releaseVal (k,v) = do
          locker <- liftIO $ atomically (takeTMVar locker_)
          case Map.lookup k locker of
            Nothing -> error "withMany_ (phase4): invariant broken"
            Just (n,Nothing) -> liftIO $ atomically $ putTMVar locker_ locker
            Just (n,Just tv)
              | n == 1 -> do
                liftIO $ atomically $ putTMVar locker_ $! Map.insert k (0,Nothing) locker
                release k v
                liftIO $ atomically $ do
                  locker <- takeTMVar locker_
                  putTMVar locker_ $! Map.delete k locker
              | otherwise -> 
                liftIO $ atomically $ do
                  putTMVar tv v
                  putTMVar locker_ $! Map.insert k (n - 1,Just tv) locker

-- | Deduplication of resource acquisition. Useful for resources shared between an
-- arbitrary number of threads where the cost of resource acquisition is high.
--
-- This is a solution to the dining philosopher problem when forks require
-- creation before use and are too bulky to keep around when not in use.
--
-- This implementation does reasonably well in both contentious and
-- non-contentious cases. In non-contentious cases, it will scale well with
-- core count, but in contentious cases, high core count is likely to degrade
-- performance.
--
-- Example uses:
--
-- * Open one or more simultaneous TCP connection and use them multiple times
--   across threads before closing them, without coordination (other than the
--   shared Locker).
--
-- * Acquire one or more simultaneous resources via disk IO and use them 
--   multiple times across threads before serializing back to disk, without
--   coordination (other than the shared Locker).
--
-- * A backing structure to implement remote access control.
--
-- WARNING: Do not let resources escape the scope of withMany_.
--
-- WARNING: Do not nest uses of multiple lockers. Instead, use a sum type to
--          unify the resource types and acquire all resources via a single 
--          `withMany_` call.
--
-- WARNING: Be sure that `acquire` and `release` are singleton for a given 
--          locker. (Solved by `Resource` in `withMany`, etc....)
--
-- NOTE: Exceptions in the `action` are rethrown after cleanup.
--
-- NOTE: If you need to protect against `throwTo`, use a green thread.
--
withMany_ :: forall k v a m. (MonadIO m, Ord k, MonadCatch m) => (k -> m v) -> (k -> v -> m ()) -> Locker k v -> [k] -> (Map k v -> m (a,Map k v)) -> m a
withMany_ acquire release locker_ keys action = do
  !vals <- acquireMany acquire locker_ keys
  !x    <- work vals 
  !r    <- releaseMany release locker_ (either (const vals) snd x)
  case x of
    Left se -> throwM se
    Right (r,_) -> pure r
  where
    -- Run the supplied action with the acquired values
    work :: Map k v -> m (Either SomeException (a,Map k v))
    work = try . action
{-# INLINABLE withMany_ #-}

-- | A variant of `withMany_` that uses `Resource` to simplify acquisition and
-- release of resources.
withMany :: (Ord k, Resource k v, MonadCatch m, MonadIO m) => Locker k v -> [k] -> (Map k v -> m (a,Map k v)) -> m a
withMany = withMany_ (liftIO . acquire) (\k v -> liftIO $ release k v)
{-# INLINE withMany #-}

-- | A variant of `withMany_` that treats the resources `v` as mutable.
withManyMutable_ :: (Ord k, MonadCatch m, MonadIO m) => (k -> m v) -> (k -> v -> m ()) -> Locker k v -> [k] -> (Map k v -> m a) -> m a
withManyMutable_ acquire release locker_ keys action = 
  withMany_ acquire release locker_ keys (\m -> action m >>= \a -> pure (a,m))
{-# INLINE withManyMutable_ #-}

-- | A variant of `withManyMutable_` that uses `Resource` to simplify
-- acquisition and release of resources.
withManyMutable :: (Ord k, Resource k v, MonadCatch m, MonadIO m) => Locker k v -> [k] -> (Map k v -> m a) -> m a
withManyMutable locker_ keys action = 
  withMany_ (liftIO . acquire) (\k v -> liftIO (release k v)) locker_ keys (\m -> action m >>= \a -> pure (a,m))
{-# INLINE withManyMutable #-}

-- | A variant of `withMany_` that acquires a single resource.
with_ :: (Ord k, MonadCatch m, MonadIO m) => (k -> m v) -> (k -> v -> m ()) -> Locker k v -> k -> (v -> m (a,v)) -> m a
with_ acquire release locker_ key action = 
  withMany_ acquire release locker_ [key] (\m -> action (m Map.! key) >>= \(a,v) -> pure (a,Map.singleton key v))
{-# INLINE with_ #-}

-- | A variant of `with_` that uses `Resource` to simplify acquisition and
-- release of the resource.
with :: (Ord k, Resource k v, MonadCatch m, MonadIO m) => Locker k v -> k -> (v -> m (a,v)) -> m a
with locker_ key action = 
  withMany_ (liftIO . acquire) (\k v -> liftIO (release k v)) locker_ [key] (\m -> action (m Map.! key) >>= \(a,v) -> pure (a,Map.singleton key v))
{-# INLINE with #-}

-- | A variant of `withMany_` that acquires a single resource and treats the
-- resource `v` as mutable.
withMutable_ :: (Ord k, MonadCatch m, MonadIO m) => (k -> m v) -> (k -> v -> m ()) -> Locker k v -> k -> (v -> m a) -> m a
withMutable_ acquire release locker_ key action = 
  withMany_ acquire release locker_ [key] (\m -> action (m Map.! key) >>= \a -> pure (a,m))
{-# INLINE withMutable_ #-}

-- | A variant of `withMutable_` that uses `Resource` to simplify acquisition
-- and release of the resource.
withMutable :: (Ord k, Resource k v, MonadCatch m, MonadIO m) => Locker k v -> k -> (v -> m a) -> m a
withMutable locker_ key action = 
  withManyMutable locker_ [key] (action . (Map.! key))
{-# INLINE withMutable #-}

